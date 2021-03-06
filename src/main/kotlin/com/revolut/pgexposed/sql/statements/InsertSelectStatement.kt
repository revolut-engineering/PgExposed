package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.sql.Column
import com.revolut.pgexposed.sql.IColumnType
import com.revolut.pgexposed.sql.Query
import com.revolut.pgexposed.sql.Transaction
import java.sql.PreparedStatement

open class InsertSelectStatement(val columns: List<Column<*>>, val selectQuery: Query, val isIgnore: Boolean = false): Statement<Int>(StatementType.INSERT, listOf(columns.first().table)) {

    init {
        if (columns.isEmpty()) error("Can't insert without provided columns")
        val tables = columns.distinctBy { it.table }
        if (tables.count() > 1) error("Can't insert to different tables ${tables.joinToString { it.name }} from single select")
        if (columns.size != selectQuery.set.fields.size) error("Columns count doesn't equal to query columns count")
    }


    override fun PreparedStatement.executeInternal(transaction: Transaction): Int? = executeUpdate()

    override fun arguments(): Iterable<Iterable<Pair<IColumnType, Any?>>> = selectQuery.arguments()

    override fun prepareSQL(transaction: Transaction): String =
        transaction.db.dialect.functionProvider.insert(isIgnore, targets.single(), columns, selectQuery.prepareSQL(transaction), transaction)
}
