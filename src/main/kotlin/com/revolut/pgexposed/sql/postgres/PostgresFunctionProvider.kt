package com.revolut.pgexposed.sql.postgres

import com.revolut.pgexposed.exceptions.throwUnsupportedException
import com.revolut.pgexposed.sql.*
import com.revolut.pgexposed.sql.transactions.TransactionManager

object PostgresFunctionProvider {

    private const val DEFAULT_VALUE_EXPRESSION = "DEFAULT VALUES"

    fun<T:String?> substring(expr: Expression<T>, start: Expression<Int>, length: Expression<Int>, builder: QueryBuilder) : String =
            "SUBSTRING(${expr.toSQL(builder)}, ${start.toSQL(builder)}, ${length.toSQL(builder)})"

    fun random(seed: Int?): String = "RANDOM(${seed?.toString().orEmpty()})"

    fun cast(expr: Expression<*>, type: IColumnType, builder: QueryBuilder) = "CAST(${expr.toSQL(builder)} AS ${type.sqlType()})"

    fun<T:String?> ExpressionWithColumnType<T>.match(pattern: String): Op<Boolean> = with(SqlExpressionBuilder) { this@match.like(pattern) }

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
    }
}