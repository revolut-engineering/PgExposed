package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.sql.*
import java.util.*

open class BatchInsertStatement(table: Table, ignore: Boolean = false): InsertStatement<List<ResultRow>>(table, ignore) {

    protected val data = ArrayList<MutableMap<Column<*>, Any?>>()

    fun addBatch() {
        if (data.isNotEmpty()) {
            data[data.size - 1] = LinkedHashMap(values)
            values.clear()
        }
        data.add(values)
        arguments = null
    }

    override fun prepareSQL(transaction: Transaction): String {
        val builder = QueryBuilder(true)
        val values = arguments!!

        val sql = if(values.isEmpty()) "" else {
            values.joinToString(prefix = "VALUES ", separator = ", ") { row ->
                row.joinToString(prefix = "(", postfix = ")", separator = ", ") { (col, value) ->
                    builder.registerArgument(col, value)
                }
            }
        }
        return transaction.db.dialect.functionProvider
                .insert(isIgnore, table, values.first().map { it.first }, sql, transaction)
    }

    private fun allColumnsInDataSet() = data.fold(setOf<Column<*>>()) { columns, row ->
        columns + row.keys
    }

    override var arguments: List<List<Pair<Column<*>, Any?>>>? = null
        get() = field ?: run {
            val nullableColumns = allColumnsInDataSet().filter { it.columnType.nullable }
            data.map { single ->
                val valuesAndDefaults = super.valuesAndDefaults(single)
                (valuesAndDefaults + (nullableColumns - valuesAndDefaults.keys).associate { it to null }).toList().sortedBy { it.first }
            }.apply { field = this }
        }

    override fun valuesAndDefaults(values: Map<Column<*>, Any?>) = arguments!!.first().toMap()
}
