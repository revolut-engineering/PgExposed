package com.revolut.pgexposed.sql

import com.revolut.pgexposed.sql.postgres.PostgresFunctionProvider
import com.revolut.pgexposed.sql.transactions.TransactionManager
import com.revolut.pgexposed.sql.postgres.currentDialect
import com.revolut.pgexposed.sql.postgres.currentDialectIfAvailable

private val comparator = compareBy<Column<*>>({ it.table.tableName }, { it.name })

class Column<T>(val table: Table, val name: String, override val columnType: IColumnType) : ExpressionWithColumnType<T>(), DdlAware, Comparable<Column<*>> {
    var referee: Column<*>? = null

    internal var onUpdate: ReferenceOption? = null
        get() = field ?: currentDialectIfAvailable?.defaultReferenceOption
    internal var onDelete: ReferenceOption? = null
                get() = field ?: currentDialectIfAvailable?.defaultReferenceOption
    internal var indexInPK: Int? = null
    internal var defaultValueFun: (() -> T)? = null
    internal var dbDefaultValue: Expression<T>? = null

    override fun equals(other: Any?): Boolean {
        return (other as? Column<*>)?.let {
            it.table == table && it.name == name && it.columnType == columnType
        } ?: false
    }

    override fun hashCode(): Int = table.hashCode()*31 + name.hashCode()

    override fun toString(): String = "${table.javaClass.name}.$name"

    override fun toSQL(queryBuilder: QueryBuilder): String = TransactionManager.current().fullIdentity(this)

    val ddl: List<String>
        get() = createStatement()

    override fun createStatement(): List<String> {
        val alterTablePrefix = "ALTER TABLE ${TransactionManager.current().identity(table)} ADD"
        val isLastColumnInPK = indexInPK != null && indexInPK == table.columns.mapNotNull { indexInPK }.max()
        val columnDefinition = when {
            !isOneColumnPK() && isLastColumnInPK -> ", ADD ${table.primaryKeyConstraint()}"
            else -> descriptionDdl()
        }

        return listOfNotNull("$alterTablePrefix $columnDefinition")
    }

    override fun modifyStatement() = listOf("ALTER TABLE ${TransactionManager.current().identity(table)} ${currentDialect.modifyColumn(this)}")

    override fun dropStatement() = listOf(TransactionManager.current().let {"ALTER TABLE ${it.identity(table)} DROP COLUMN ${it.identity(this)}" })

    internal fun isOneColumnPK() = table.columns.singleOrNull { it.indexInPK != null } == this

    fun descriptionDdl(): String = buildString {
        val tr = TransactionManager.current()
        append(tr.identity(this@Column))
        append(" ")
        val isPKColumn = indexInPK != null
        val colType = columnType
        append(colType.sqlType())

        val _dbDefaultValue = dbDefaultValue
        if (!isPKColumn && _dbDefaultValue != null) {
            val expressionSQL = PostgresFunctionProvider.processForDefaultValue(_dbDefaultValue)
            append(" DEFAULT $expressionSQL" )
        }

        if (colType.nullable) {
            append(" NULL")
        } else if (!isPKColumn) {
            append(" NOT NULL")
        }

        if (isOneColumnPK()) {
            append(" PRIMARY KEY")
        }
    }

    override fun compareTo(other: Column<*>): Int = comparator.compare(this, other)
}