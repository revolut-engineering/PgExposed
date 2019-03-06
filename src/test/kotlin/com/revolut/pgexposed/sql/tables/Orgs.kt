package com.revolut.pgexposed.sql.tables

import com.revolut.pgexposed.sql.Table
import java.util.*

@Suppress("unused")
object Orgs : Table("orgs") {
    val id = integer("id").autoIncrement().primaryKey()
    val uid = varchar("uid", 36).uniqueIndex().clientDefault { UUID.randomUUID().toString() }
    val name = varchar("name", 256)
}