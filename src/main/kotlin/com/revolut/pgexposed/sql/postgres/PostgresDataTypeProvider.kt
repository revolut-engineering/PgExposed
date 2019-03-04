package com.revolut.pgexposed.sql.postgres

import com.revolut.pgexposed.sql.Expression
import com.revolut.pgexposed.sql.LiteralOp
import com.revolut.pgexposed.sql.QueryBuilder
import java.util.*

object PostgresDataTypeProvider {
    fun shortAutoincType(): String = "SERIAL"

    fun shortType() = "INT"

    fun longAutoincType(): String = "BIGSERIAL"

    fun longType() = "BIGINT"

    fun floatType() = "FLOAT"

    fun doubleType() = "DOUBLE PRECISION"

    fun uuidType(): String = "uuid"

    fun dateTimeType(): String = "TIMESTAMP"

    fun blobType(): String = "bytea"

    fun binaryType(length: Int): String = "bytea"

    fun booleanType(): String = "BOOLEAN"

    fun booleanToStatementString(bool: Boolean) = bool.toString()

    fun uuidToDB(value: UUID): Any = value

    fun booleanFromStringToBoolean(value: String): Boolean = value.toBoolean()

    fun textType() = "TEXT"

    val blobAsStream = true

    fun processForDefaultValue(e: Expression<*>) : String = when (e) {
        is LiteralOp<*> -> e.toSQL(QueryBuilder(false))
        else -> "(${e.toSQL(QueryBuilder(false))})"
    }
}