package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.sql.Column
import com.revolut.pgexposed.sql.ResultRow
import com.revolut.pgexposed.sql.Table
import com.revolut.pgexposed.sql.isAutoInc
import com.revolut.pgexposed.sql.transactions.TransactionManager
import java.util.*

open class BatchInsertStatement(table: Table, ignore: Boolean = false): InsertStatement<List<ResultRow>>(table, ignore) {

    override val isAlwaysBatch = true

    protected val data = ArrayList<MutableMap<Column<*>, Any?>>()

    private fun Column<*>.isDefaultable() = columnType.nullable || defaultValueFun != null

    override operator fun <S> set(column: Column<S>, value: S) {
        if (data.size > 1 && column !in data[data.size - 2] && !column.isDefaultable()) {
            throw BatchDataInconsistentException("Can't set $value for ${TransactionManager.current().fullIdentity(column)} because previous insertion can't be defaulted for that column.")
        }
        super.set(column, value)
    }

    fun addBatch() {
        if (data.isNotEmpty()) {
            validateLastBatch()
            data[data.size - 1] = LinkedHashMap(values)
            values.clear()
        }
        data.add(values)
        arguments = null
    }

    internal open fun validateLastBatch() {
        val cantBeDefaulted = (data.last().keys - values.keys).filterNot { it.isDefaultable() }
        if (cantBeDefaulted.isNotEmpty()) {
            val columnList = cantBeDefaulted.joinToString { TransactionManager.current().fullIdentity(it) }
            throw BatchDataInconsistentException("Can't add new batch because columns: $columnList don't have client default values. DB defaults don't support in batch inserts")
        }
        val requiredInTargets = (targets.flatMap { it.columns } - values.keys).filter { !it.isDefaultable() && !it.columnType.isAutoInc }
        if (requiredInTargets.any()) {
            throw BatchDataInconsistentException("Can't add new batch because columns: ${requiredInTargets.joinToString()} don't have client default values. DB defaults don't support in batch inserts")
        }
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
