package org.pgexposed.sql.statements

import org.pgexposed.sql.*
import java.sql.PreparedStatement

open class DeleteStatement(val table: Table, val where: Op<Boolean>? = null): Statement<Int>(StatementType.DELETE, listOf(table)) {

    override fun PreparedStatement.executeInternal(transaction: Transaction): Int = executeUpdate()

    override fun prepareSQL(transaction: Transaction): String =
        transaction.db.dialect.functionProvider.delete(table, where?.toSQL(QueryBuilder(true)), transaction)

    override fun arguments(): Iterable<Iterable<Pair<IColumnType, Any?>>> = QueryBuilder(true).run {
        where?.toSQL(this)
        listOf(args)
    }

    companion object {
        fun where(transaction: Transaction, table: Table, op: Op<Boolean>): Int
            = DeleteStatement(table, op).execute(transaction) ?: 0

        fun all(transaction: Transaction, table: Table): Int = DeleteStatement(table).execute(transaction) ?: 0
    }
}
