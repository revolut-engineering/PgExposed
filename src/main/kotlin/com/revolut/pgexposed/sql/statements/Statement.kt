package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.exceptions.ExposedSQLException
import com.revolut.pgexposed.sql.IColumnType
import com.revolut.pgexposed.sql.Table
import com.revolut.pgexposed.sql.Transaction
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.*

abstract class Statement<out T>(val type: StatementType, val targets: List<Table>) {

    abstract fun PreparedStatement.executeInternal(transaction: Transaction): T?

    abstract fun prepareSQL(transaction: Transaction): String

    abstract fun arguments(): Iterable<Iterable<Pair<IColumnType, Any?>>>

    open fun prepared(transaction: Transaction, sql: String) : PreparedStatement =
        transaction.connection.prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS)!!

    fun execute(transaction: Transaction): T? = transaction.exec(this)

    internal fun executeIn(transaction: Transaction): Pair<T?, List<StatementContext>> {
        val arguments = arguments()
        val contexts = when {
            arguments.count() > 0 ->
                StatementContext(this, arguments.flatten()).let { context ->
                    transaction.interceptors.forEach { it.beforeExecution(transaction, context) }
                    listOf(context)
                }
            else -> {
                val context = StatementContext(this, emptyList())
                transaction.interceptors.forEach { it.beforeExecution(transaction, context) }
                listOf(context)
            }
        }

        val statement = try {
            prepared(transaction, prepareSQL(transaction))
        } catch (e: SQLException) {
            throw ExposedSQLException(e, contexts, transaction)
        }
        contexts.forEachIndexed { _, context ->
            statement.fillParameters(context.args)
        }
        if (!transaction.db.supportsMultipleResultSets) transaction.closeExecutedStatements()

        transaction.currentStatement = statement
        val result = try {
            statement.executeInternal(transaction)
        } catch (e: SQLException) {
            throw ExposedSQLException(e, contexts, transaction)
        }
        transaction.currentStatement = null
        transaction.executedStatements.add(statement)

        transaction.interceptors.forEach { it.afterExecution(transaction, contexts, statement) }
        return result to contexts
    }
}

fun StatementContext.expandArgs(transaction: Transaction) : String {
    val sql = sql(transaction)
    val iterator = args.iterator()
    if (!iterator.hasNext())
        return sql

    return buildString {
        val quoteStack = Stack<Char>()
        var lastPos = 0
        for (i in 0 until sql.length) {
            val char = sql[i]
            if (char == '?') {
                if (quoteStack.isEmpty()) {
                    append(sql.substring(lastPos, i))
                    lastPos = i + 1
                    val (col, value) = iterator.next()
                    append(col.valueToString(value))
                }
                continue
            }

            if (char == '\'' || char == '\"') {
                if (quoteStack.isEmpty()) {
                    quoteStack.push(char)
                } else {
                    val currentQuote = quoteStack.peek()
                    if (currentQuote == char)
                        quoteStack.pop()
                    else
                        quoteStack.push(char)
                }
            }
        }

        if (lastPos < sql.length)
            append(sql.substring(lastPos))
    }
}

fun PreparedStatement.fillParameters(args: Iterable<Pair<IColumnType, Any?>>): Int {
    args.forEachIndexed { index, (c, v) ->
        c.setParameter(this, index + 1, c.valueToDB(v))
    }

    return args.count() + 1
}

enum class StatementGroup {
    DDL, DML
}

enum class StatementType(val group: StatementGroup) {
    INSERT(StatementGroup.DML), UPDATE(StatementGroup.DML), DELETE(StatementGroup.DML), SELECT(StatementGroup.DML),
    CREATE(StatementGroup.DDL), ALTER(StatementGroup.DDL), TRUNCATE(StatementGroup.DDL), DROP(StatementGroup.DDL),
    GRANT(StatementGroup.DDL), OTHER(StatementGroup.DDL)
}