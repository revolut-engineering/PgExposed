package org.pgexposed.sql.statements

import org.pgexposed.sql.Column
import org.pgexposed.sql.Expression
import org.pgexposed.sql.Table
import org.pgexposed.sql.VarCharColumnType
import java.util.*

/**
 * @author max
 */

abstract class UpdateBuilder<out T>(type: StatementType, targets: List<Table>): Statement<T>(type, targets) {
    protected val values: MutableMap<Column<*>, Any?> = LinkedHashMap()

    open operator fun <S> set(column: Column<S>, value: S) {
        if (values.containsKey(column)) {
            error("$column is already initialized")
        }
        if (!column.columnType.nullable && value == null) {
            error("Trying to set null to not nullable column $column")
        }
        if (column.columnType is VarCharColumnType && value is String && value.length > column.columnType.colLength) {
            error("Value '$value' can't be stored to database column because exceeds length $column.columnType.colLength")
        }
        values[column] = value
    }

    open fun <T, S:T?> update(column: Column<T>, value: Expression<S>) {
        if (values.containsKey(column)) {
            error("$column is already initialized")
        }
        values[column] = value
    }
}
