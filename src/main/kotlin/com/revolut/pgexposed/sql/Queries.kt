package com.revolut.pgexposed.sql

import com.revolut.pgexposed.sql.postgres.currentDialect
import com.revolut.pgexposed.sql.statements.*
import com.revolut.pgexposed.sql.transactions.TransactionManager
import java.util.*

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testSelect01
 */
inline fun FieldSet.select(where: SqlExpressionBuilder.()->Op<Boolean>) : Query = select(SqlExpressionBuilder.where())

fun FieldSet.select(where: Op<Boolean>) : Query = Query(this, where)

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testSelectDistinct
 */
fun FieldSet.selectAll() : Query = Query(this, null)

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testDelete01
 */
fun Table.deleteWhere(op: SqlExpressionBuilder.() -> Op<Boolean>) =
    DeleteStatement.where(TransactionManager.current(), this@deleteWhere, SqlExpressionBuilder.op())

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testDelete01
 */
fun Table.deleteAll() =
    DeleteStatement.all(TransactionManager.current(), this@deleteAll)

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testInsert01
 */
fun <T:Table> T.insert(body: T.(InsertStatement<Number>)->Unit): InsertStatement<Number> = InsertStatement<Number>(this).apply {
    body(this)
    execute(TransactionManager.current())
}

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testBatchInsert01
 */
fun <T:Table, E:Any> T.batchInsert(data: Iterable<E>, ignore: Boolean = false, body: BatchInsertStatement.(E)->Unit): List<ResultRow> {
    if (data.count() == 0) return emptyList()

    val statement = BatchInsertStatement(this, ignore)
    val result = ArrayList<ResultRow>()

    for (element in data) {
        statement.addBatch()
        statement.body(element)
    }
    if (statement.arguments().isNotEmpty()) {
        statement.execute(TransactionManager.current())
        result += statement.resultedValues.orEmpty()
    }
    return result
}

fun <T:Table> T.insertIgnore(body: T.(UpdateBuilder<*>)->Unit): InsertStatement<Long> = InsertStatement<Long>(this, isIgnore = true).apply {
    body(this)
    execute(TransactionManager.current())
}

fun <T:Table> T.replace(body: T.(UpdateBuilder<*>)->Unit): ReplaceStatement<Long> = ReplaceStatement<Long>(this).apply {
    body(this)
    execute(TransactionManager.current())
}

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testInsertSelect01
 */
fun <T:Table> T.insert(selectQuery: Query, columns: List<Column<*>> = this.columns.filterNot { it.columnType.isAutoInc }) =
    InsertSelectStatement(columns, selectQuery).execute(TransactionManager.current())


fun <T:Table> T.insertIgnore(selectQuery: Query, columns: List<Column<*>> = this.columns.filterNot { it.columnType.isAutoInc }) =
    InsertSelectStatement(columns, selectQuery, true).execute(TransactionManager.current())


/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DMLTests.testUpdate01
 */
fun <T:Table> T.update(where: (SqlExpressionBuilder.() -> Op<Boolean>)? = null, body: T.(UpdateStatement) -> Unit): Int {
    val query = UpdateStatement(this, where?.let { SqlExpressionBuilder.it() })
    body(query)
    return query.execute(TransactionManager.current())!!
}

fun Join.update(where: (SqlExpressionBuilder.() -> Op<Boolean>)? = null, body: (UpdateStatement) -> Unit) : Int {
    val query = UpdateStatement(this, where?.let { SqlExpressionBuilder.it() })
    body(query)
    return query.execute(TransactionManager.current())!!
}

/**
 * @sample com.revolut.pgexposed.sql.tests.shared.DDLTests.tableExists02
 */
fun Table.exists(): Boolean = currentDialect.tableExists(this)

/**
 * Log Exposed table mappings <-> real database mapping problems and returns DDL Statements to fix them
 */
fun checkMappingConsistence(vararg tables: Table): List<String> {
    checkExcessiveIndices(*tables)
    return checkMissingIndices(*tables).flatMap { it.createStatement() }
}

fun checkExcessiveIndices(vararg tables: Table) {

    val excessiveConstraints = currentDialect.columnConstraints(*tables).filter { it.value.size > 1 }

    if (!excessiveConstraints.isEmpty()) {
        exposedLogger.warn("List of excessive foreign key constraints:")
        excessiveConstraints.forEach { (pair, fk) ->
            val constraint = fk.first()
            exposedLogger.warn("\t\t\t'${pair.first}'.'${pair.second}' -> '${constraint.fromTable}'.'${constraint.fromColumn}':\t${fk.joinToString(", ") {it.fkName}}")
        }

        exposedLogger.info("SQL Queries to remove excessive keys:")
        excessiveConstraints.forEach {
            it.value.take(it.value.size - 1).forEach {
                exposedLogger.info("\t\t\t${it.dropStatement()};")
            }
        }
    }

    val excessiveIndices = currentDialect.existingIndices(*tables).flatMap { it.value }.groupBy { Triple(it.table, it.unique, it.columns.joinToString { it.name }) }.filter { it.value.size > 1}
    if (!excessiveIndices.isEmpty()) {
        exposedLogger.warn("List of excessive indices:")
        excessiveIndices.forEach { (triple, indices)->
            exposedLogger.warn("\t\t\t'${triple.first.tableName}'.'${triple.third}' -> ${indices.joinToString(", ") {it.indexName}}")
        }
        exposedLogger.info("SQL Queries to remove excessive indices:")
        excessiveIndices.forEach {
            it.value.take(it.value.size - 1).forEach {
                exposedLogger.info("\t\t\t${it.dropStatement()};")
            }
        }
    }
}

/** Returns list of indices missed in database **/
private fun checkMissingIndices(vararg tables: Table): List<Index> {
    fun Collection<Index>.log(mainMessage: String) {
        if (isNotEmpty()) {
            exposedLogger.warn(mainMessage)
            forEach {
                exposedLogger.warn("\t\t$it")
            }
        }
    }

    val existingIndices = currentDialect.existingIndices(*tables)
    val missingIndices = HashSet<Index>()
    val notMappedIndices = HashMap<String, MutableSet<Index>>()
    val nameDiffers = HashSet<Index>()
    for (table in tables) {
        val existingTableIndices = existingIndices[table].orEmpty()
        val mappedIndices = table.indices

        existingTableIndices.forEach { index ->
            mappedIndices.firstOrNull { it.onlyNameDiffer(index) }?.let {
                exposedLogger.trace("Index on table '${table.tableName}' differs only in name: in db ${index.indexName} -> in mapping ${it.indexName}")
                nameDiffers.add(index)
                nameDiffers.add(it)
            }
        }

        notMappedIndices.getOrPut(table.nameInDatabaseCase()) {hashSetOf()}.addAll(existingTableIndices.subtract(mappedIndices))

        missingIndices.addAll(mappedIndices.subtract(existingTableIndices))
    }

    val toCreate = missingIndices.subtract(nameDiffers)
    toCreate.log("Indices missed from database (will be created):")
    notMappedIndices.forEach { (name, indexes) -> indexes.subtract(nameDiffers).log("Indices exist in database and not mapped in code on class '$name':") }
    return toCreate.toList()
}
