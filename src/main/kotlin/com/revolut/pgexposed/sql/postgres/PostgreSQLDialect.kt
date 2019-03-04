package com.revolut.pgexposed.sql.postgres

import com.revolut.pgexposed.sql.*
import com.revolut.pgexposed.sql.transactions.TransactionManager
import java.sql.ResultSet
import java.util.ArrayList
import java.util.HashMap

class PostgreSQLDialect : DatabaseDialect {

    override val name = "postgresql"
    override val dataTypeProvider = PostgresDataTypeProvider
    override val functionProvider = PostgresFunctionProvider

    /* Cached values */
    private var _allTableNames: List<String>? = null
    val allTablesNames: List<String>
        get() {
            if (_allTableNames == null) {
                _allTableNames = allTablesNames()
            }
            return _allTableNames!!
        }

    private val isUpperCaseIdentifiers by lazy { TransactionManager.current().db.metadata.storesUpperCaseIdentifiers() }
    private val isLowerCaseIdentifiers by lazy { TransactionManager.current().db.metadata.storesLowerCaseIdentifiers() }
    val String.inProperCase: String get() = when {
        isUpperCaseIdentifiers -> toUpperCase()
        isLowerCaseIdentifiers -> toLowerCase()
        else -> this
    }

    /* Method always re-read data from DB. Using allTablesNames field is preferred way */
    override fun allTablesNames(): List<String> {
        val result = ArrayList<String>()
        val tr = TransactionManager.current()
        val resultSet = tr.db.metadata.getTables(getDatabase(), null, "%", arrayOf("TABLE"))

        while (resultSet.next()) {
            result.add(resultSet.getString("TABLE_NAME").inProperCase)
        }
        resultSet.close()
        return result
    }

    override fun getDatabase(): String = catalog(TransactionManager.current())

    override fun tableExists(table: Table) = allTablesNames.any { it == table.nameInDatabaseCase() }

    protected fun ResultSet.extractColumns(tables: Array<out Table>, extract: (ResultSet) -> Pair<String, ColumnMetadata>): Map<Table, List<ColumnMetadata>> {
        val mapping = tables.associateBy { it.nameInDatabaseCase() }
        val result = HashMap<Table, MutableList<ColumnMetadata>>()

        while (next()) {
            val (tableName, columnMetadata) = extract(this)
            mapping[tableName]?.let { t ->
                result.getOrPut(t) { arrayListOf() } += columnMetadata
            }
        }
        return result
    }

    override fun tableColumns(vararg tables: Table): Map<Table, List<ColumnMetadata>> {
        val rs = TransactionManager.current().db.metadata.getColumns(getDatabase(), null, "%", "%")
        val result = rs.extractColumns(tables) {
            it.getString("TABLE_NAME") to ColumnMetadata(it.getString("COLUMN_NAME"), it.getInt("DATA_TYPE"), it.getBoolean("NULLABLE"))
        }
        rs.close()
        return result
    }

    private val columnConstraintsCache = HashMap<String, List<ForeignKeyConstraint>>()

    protected fun String.quoteIdentifierWhenWrongCaseOrNecessary(tr: Transaction)
            = if (tr.db.shouldQuoteIdentifiers && inProperCase != this) "${tr.db.identityQuoteString}$this${tr.db.identityQuoteString}" else tr.quoteIfNecessary(this)

    @Synchronized
    override fun columnConstraints(vararg tables: Table): Map<Pair<String, String>, List<ForeignKeyConstraint>> {
        val constraints = HashMap<Pair<String, String>, MutableList<ForeignKeyConstraint>>()
        val tr = TransactionManager.current()

        tables.map{ it.nameInDatabaseCase() }.forEach { table ->
            columnConstraintsCache.getOrPut(table) {
                val rs = tr.db.metadata.getImportedKeys(getDatabase(), null, table)
                val tableConstraint = arrayListOf<ForeignKeyConstraint> ()
                while (rs.next()) {
                    val fromTableName = rs.getString("FKTABLE_NAME")!!
                    val fromColumnName = rs.getString("FKCOLUMN_NAME")!!.quoteIdentifierWhenWrongCaseOrNecessary(tr)
                    val constraintName = rs.getString("FK_NAME")!!
                    val targetTableName = rs.getString("PKTABLE_NAME")!!
                    val targetColumnName = rs.getString("PKCOLUMN_NAME")!!.quoteIdentifierWhenWrongCaseOrNecessary(tr)
                    val constraintUpdateRule = ReferenceOption.resolveRefOptionFromJdbc(rs.getInt("UPDATE_RULE"))
                    val constraintDeleteRule = ReferenceOption.resolveRefOptionFromJdbc(rs.getInt("DELETE_RULE"))
                    tableConstraint.add(
                            ForeignKeyConstraint(constraintName,
                                    targetTableName, targetColumnName,
                                    fromTableName, fromColumnName,
                                    constraintUpdateRule, constraintDeleteRule)
                    )
                }
                rs.close()
                tableConstraint
            }.forEach { it ->
                constraints.getOrPut(it.fromTable to it.fromColumn){arrayListOf()}.add(it)
            }

        }
        return constraints
    }

    private val existingIndicesCache = HashMap<Table, List<Index>>()

    @Synchronized
    override fun existingIndices(vararg tables: Table): Map<Table, List<Index>> {
        for(table in tables) {
            val tableName = table.nameInDatabaseCase()
            val transaction = TransactionManager.current()
            val metadata = transaction.db.metadata

            existingIndicesCache.getOrPut(table) {
                val pkNames = metadata.getPrimaryKeys(getDatabase(), null, tableName).let { rs ->
                    val names = arrayListOf<String>()
                    while(rs.next()) {
                        rs.getString("PK_NAME")?.let { names += it }
                    }
                    rs.close()
                    names
                }
                val rs = metadata.getIndexInfo(getDatabase(), null, tableName, false, false)

                val tmpIndices = hashMapOf<Pair<String, Boolean>, MutableList<String>>()

                while (rs.next()) {
                    rs.getString("INDEX_NAME")?.let {
                        val column = transaction.quoteIfNecessary(rs.getString("COLUMN_NAME")!!)
                        val isUnique = !rs.getBoolean("NON_UNIQUE")
                        tmpIndices.getOrPut(it to isUnique) { arrayListOf() }.add(column)
                    }
                }
                rs.close()
                val tColumns = table.columns.associateBy { transaction.identity(it) }
                tmpIndices.filterNot { it.key.first in pkNames }
                        .mapNotNull { (index, columns) ->
                            columns.mapNotNull { cn -> tColumns[cn] }.takeIf { c -> c.size == columns.size }?.let { c -> Index(c, index.second, index.first) }
                        }
            }
        }
        return HashMap(existingIndicesCache)
    }

    @Synchronized
    override fun resetCaches() {
        _allTableNames = null
        columnConstraintsCache.clear()
        existingIndicesCache.clear()
    }

    override fun createIndex(index: Index): String {
        val t = TransactionManager.current()
        val quotedTableName = t.identity(index.table)
        val quotedIndexName = t.quoteIfNecessary(t.cutIfNecessary(index.indexName))
        val columnsList = index.columns.joinToString(prefix = "(", postfix = ")") { t.identity(it) }
        return if (index.unique) {
            "ALTER TABLE $quotedTableName ADD CONSTRAINT $quotedIndexName UNIQUE $columnsList"
        } else {
            "CREATE INDEX $quotedIndexName ON $quotedTableName $columnsList"
        }

    }

    override fun dropIndex(tableName: String, indexName: String): String {
        val t = TransactionManager.current()
        return "ALTER TABLE ${t.quoteIfNecessary(tableName)} DROP CONSTRAINT ${t.quoteIfNecessary(indexName)}"
    }

    private val supportsSelectForUpdate by lazy { TransactionManager.current().db.metadata.supportsSelectForUpdate() }
    override fun supportsSelectForUpdate() = supportsSelectForUpdate

    override val supportsMultipleGeneratedKeys: Boolean = true

    override fun modifyColumn(column: Column<*>): String = buildString {
        val colName = TransactionManager.current().identity(column)
        append("ALTER COLUMN $colName TYPE ${column.columnType.sqlType()},")
        append("ALTER COLUMN $colName ")
        if (column.columnType.nullable)
            append("DROP ")
        else
            append("SET ")
        append("NOT NULL")
        column.dbDefaultValue?.let {
            append(", ALTER COLUMN $colName SET DEFAULT ${PostgresDataTypeProvider.processForDefaultValue(it)}")
        }
    }

}