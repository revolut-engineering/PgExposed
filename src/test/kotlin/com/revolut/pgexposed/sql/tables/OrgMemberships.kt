package com.revolut.pgexposed.sql.tables

import com.revolut.pgexposed.sql.Table

@Suppress("unused")
object OrgMemberships : Table("org_membership") {
    val id = integer("id").autoIncrement().primaryKey()
    val orgId = (integer("orgId") references Orgs.id)
}