package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.sql.*
import java.sql.PreparedStatement

open class UpdateStatement(val targetsSet: ColumnSet, val where: Op<Boolean>? = null): UpdateBuilder<Int>(StatementType.UPDATE, targetsSet.targetTables()) {

    open val firstDataSet: List<Pair<Column<*>, Any?>> get() = values.toList()

    override fun PreparedStatement.executeInternal(transaction: Transaction): Int {
        return if (values.isEmpty()) 0 else executeUpdate()
    }

    override fun prepareSQL(transaction: Transaction): String =
            transaction.db.dialect.functionProvider.update(targetsSet, firstDataSet, where, transaction)

    override fun arguments(): Iterable<Iterable<Pair<IColumnType, Any?>>> = QueryBuilder(true).run {
        values.forEach {
            registerArgument(it.key, it.value)
        }
        where?.toSQL(this)
        if (args.isNotEmpty()) listOf(args) else emptyList()
    }

}
