package org.pgexposed.sql.postgres

import org.pgexposed.sql.*
import org.pgexposed.sql.transactions.TransactionManager

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

internal val currentDialect: DatabaseDialect get() = TransactionManager.current().db.dialect

internal val currentDialectIfAvailable : DatabaseDialect? get() =
    if (TransactionManager.isInitialized() && TransactionManager.currentOrNull() != null) {
        currentDialect
    } else null

internal fun String.inProperCase(): String = (currentDialectIfAvailable as? PostgreSQLDialect)?.run {
    this@inProperCase.inProperCase
} ?: this
