package org.pgexposed.sql.vendors

import org.pgexposed.exceptions.throwUnsupportedException
import org.pgexposed.sql.*
import org.pgexposed.sql.transactions.TransactionManager
import java.sql.ResultSet
import java.util.*

object PostgresDataTypeProvider {
    fun shortAutoincType(): String = "SERIAL"

    fun shortType() = "INT"

    fun longAutoincType(): String = "BIGSERIAL"

    fun longType() = "BIGINT"

    fun floatType() = "FLOAT"

    fun doubleType() = "DOUBLE PRECISION"

    fun uuidType(): String = "uuid"

    fun dateTimeType(): String = "TIMESTAMP"

    fun blobType(): String = "bytea"

    fun binaryType(length: Int): String = "bytea"

    fun booleanType(): String = "BOOLEAN"

    fun booleanToStatementString(bool: Boolean) = bool.toString()

    fun uuidToDB(value: UUID): Any = value

    fun booleanFromStringToBoolean(value: String): Boolean = value.toBoolean()

    fun textType() = "TEXT"

    val blobAsStream = true

    fun processForDefaultValue(e: Expression<*>) : String = when (e) {
        is LiteralOp<*> -> e.toSQL(QueryBuilder(false))
        else -> "(${e.toSQL(QueryBuilder(false))})"
    }
}

object PostgresFunctionProvider {

    const val DEFAULT_VALUE_EXPRESSION = "DEFAULT VALUES"

    fun<T:String?> substring(expr: Expression<T>, start: Expression<Int>, length: Expression<Int>, builder: QueryBuilder) : String =
            "SUBSTRING(${expr.toSQL(builder)}, ${start.toSQL(builder)}, ${length.toSQL(builder)})"

    fun random(seed: Int?): String = "RANDOM(${seed?.toString().orEmpty()})"

    fun cast(expr: Expression<*>, type: IColumnType, builder: QueryBuilder) = "CAST(${expr.toSQL(builder)} AS ${type.sqlType()})"

    fun<T:String?> ExpressionWithColumnType<T>.match(pattern: String, mode: MatchMode? = null): Op<Boolean> = with(SqlExpressionBuilder) { this@match.like(pattern) }

    private const val onConflictIgnore = "ON CONFLICT DO NOTHING"

    fun insert(ignore: Boolean, table: Table, columns: List<Column<*>>, expr: String, transaction: Transaction): String {

        val (columnsExpr, valuesExpr) = if (columns.isNotEmpty()) {
            columns.joinToString(prefix = "(", postfix = ")") { transaction.identity(it) } to expr
        } else "" to DEFAULT_VALUE_EXPRESSION

        val def =  "INSERT INTO ${transaction.identity(table)} $columnsExpr $valuesExpr"

        return if (ignore) "$def $onConflictIgnore" else def
    }

    fun update(targets: ColumnSet, columnsAndValues: List<Pair<Column<*>, Any?>>, where: Op<Boolean>?, transaction: Transaction): String {
        return buildString {
            val builder = QueryBuilder(true)
            append("UPDATE ${targets.describe(transaction, builder)}")
            append(" SET ")
            append(columnsAndValues.joinToString { (col, value) ->
                "${transaction.identity(col)}=" + builder.registerArgument(col, value)
            })

            where?.let { append(" WHERE " + it.toSQL(builder)) }
        }
    }

    fun delete(table: Table, where: String?, transaction: Transaction): String {
        return buildString {
            append("DELETE FROM ")
            append(transaction.identity(table))
            if (where != null) {
                append(" WHERE ")
                append(where)
            }
        }
    }

    fun queryLimit(size: Int, offset: Int) = "LIMIT $size" + if (offset > 0) " OFFSET $offset" else ""

    fun replace(table: Table, data: List<Pair<Column<*>, Any?>>, transaction: Transaction): String {
        val builder = QueryBuilder(true)
        val sql = if (data.isEmpty()) ""
        else data.joinToString(prefix = "VALUES (", postfix = ")") { (col, value) ->
            builder.registerArgument(col, value)
        }

        val columns = data.map { it.first }

        val def = insert(false, table, columns, sql, transaction)

        val uniqueCols = columns.filter { it.indexInPK != null }.sortedBy { it.indexInPK }
        if (uniqueCols.isEmpty())
            transaction.throwUnsupportedException("Postgres replace table must supply at least one primary key")
        val conflictKey = uniqueCols.joinToString { transaction.identity(it) }
        return def + "ON CONFLICT ($conflictKey) DO UPDATE SET " + columns.joinToString { "${transaction.identity(it)}=EXCLUDED.${transaction.identity(it)}" }
    }

    fun <T : String?> groupConcat(expr: GroupConcat<T>, queryBuilder: QueryBuilder): String {
        val tr = TransactionManager.current()
        return when {
            expr.orderBy.isNotEmpty() -> tr.throwUnsupportedException("PostgreSQL doesn't support ORDER BY in STRING_AGG.")
            expr.distinct -> tr.throwUnsupportedException("PostgreSQL doesn't support DISTINCT in STRING_AGG.")
            expr.separator == null -> tr.throwUnsupportedException("PostgreSQL requires explicit separator in STRING_AGG.")
            else -> "STRING_AGG(${expr.expr.toSQL(queryBuilder)}, '${expr.separator}')"
        }
    }

    interface MatchMode {
        fun mode() : String
    }
}

/**
 * type:
 * @see java.sql.Types
 */
data class ColumnMetadata(val name: String, val type: Int, val nullable: Boolean)

interface DatabaseDialect {
    val name: String
    val dataTypeProvider: PostgresDataTypeProvider
    val functionProvider: PostgresFunctionProvider

    fun getDatabase(): String

    fun allTablesNames(): List<String>
    /**
     * returns list of pairs (column name + nullable) for every table
     */
    fun tableColumns(vararg tables: Table): Map<Table, List<ColumnMetadata>> = emptyMap()

    /**
     * returns map of constraint for a table name/column name pair
     */
    fun columnConstraints(vararg tables: Table): Map<Pair<String, String>, List<ForeignKeyConstraint>> = emptyMap()

    /**
     * return set of indices for each table
     */
    fun existingIndices(vararg tables: Table): Map<Table, List<Index>> = emptyMap()

    fun tableExists(table: Table): Boolean

    fun checkTableMapping(table: Table) = true

    fun resetCaches()

    fun supportsSelectForUpdate(): Boolean
    val supportsMultipleGeneratedKeys: Boolean

    fun isAllowedAsColumnDefault(e: Expression<*>) = true

    // --> REVIEW
    val supportsIfNotExists: Boolean get() = true
    val needsSequenceToAutoInc: Boolean get() = false
    val needsQuotesWhenSymbolsInNames: Boolean get() = true
    val identifierLengthLimit: Int get() = 100
    fun catalog(transaction: Transaction): String = transaction.connection.catalog
    // <-- REVIEW

    val defaultReferenceOption : ReferenceOption get() = ReferenceOption.RESTRICT

    // Specific SQL statements

    fun createIndex(index: Index): String
    fun dropIndex(tableName: String, indexName: String): String
    fun modifyColumn(column: Column<*>) : String
}

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

internal val currentDialect: DatabaseDialect get() = TransactionManager.current().db.dialect

internal val currentDialectIfAvailable : DatabaseDialect? get() =
    if (TransactionManager.isInitialized() && TransactionManager.currentOrNull() != null) {
        currentDialect
    } else null

internal fun String.inProperCase(): String = (currentDialectIfAvailable as? PostgreSQLDialect)?.run {
    this@inProperCase.inProperCase
} ?: this
