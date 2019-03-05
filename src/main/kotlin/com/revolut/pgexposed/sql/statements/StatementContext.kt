package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.sql.IColumnType
import com.revolut.pgexposed.sql.Transaction

class StatementContext(val statement: Statement<*>, val args: Iterable<Pair<IColumnType, Any?>>) {
    fun sql(transaction: Transaction) = statement.prepareSQL(transaction)
}