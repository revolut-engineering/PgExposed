@file:Suppress("PackageDirectoryMismatch")
package org.pgexposed.exceptions

import org.pgexposed.sql.Query
import org.pgexposed.sql.QueryBuilder
import org.pgexposed.sql.Transaction
import org.pgexposed.sql.postgres.DatabaseDialect
import org.pgexposed.sql.statements.StatementContext
import org.pgexposed.sql.statements.expandArgs
import java.sql.SQLException

class ExposedSQLException(cause: Throwable?, val contexts: List<StatementContext>, private val transaction: Transaction) : SQLException(cause) {
    fun causedByQueries() : List<String> = contexts.map {
        try {
            if (transaction.debug) {
                it.expandArgs(transaction)
            } else {
                it.sql(transaction)
            }
        } catch (e: Throwable) {
            try {
                (it.statement as? Query)?.prepareSQL(QueryBuilder(!transaction.debug))
            } catch (e: Throwable) {
                null
            } ?: "Failed on expanding args for ${it.statement.type}: ${it.statement}"
        }
    }

    private val originalSQLException = cause as? SQLException

    override fun getSQLState(): String  = originalSQLException?.sqlState.orEmpty()

    override fun getErrorCode(): Int = originalSQLException?.errorCode ?: 0

    override fun toString() = "${super.toString()}\nSQL: ${causedByQueries()}"
}

class UnsupportedByDialectException(baseMessage: String, val dialect: DatabaseDialect) : UnsupportedOperationException(baseMessage + ", dialect: ${dialect.name}.")

internal fun Transaction.throwUnsupportedException(message: String): Nothing = throw UnsupportedByDialectException(message, db.dialect)
