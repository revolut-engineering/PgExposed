package org.pgexposed.sql.statements

import org.pgexposed.sql.Table
import org.pgexposed.sql.Transaction

/**
 * @author max
 */
open class ReplaceStatement<Key:Any>(table: Table) : InsertStatement<Key>(table) {
    override fun prepareSQL(transaction: Transaction): String = transaction.db.dialect.functionProvider.replace(table, arguments!!.first(), transaction)
}
