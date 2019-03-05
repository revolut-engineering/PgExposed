package com.revolut.pgexposed.sql.statements

import com.revolut.pgexposed.sql.Table
import com.revolut.pgexposed.sql.Transaction

open class ReplaceStatement<Key:Any>(table: Table) : InsertStatement<Key>(table) {
    override fun prepareSQL(transaction: Transaction): String =
            transaction.db.dialect.functionProvider.replace(table, arguments!!.first(), transaction)
}
