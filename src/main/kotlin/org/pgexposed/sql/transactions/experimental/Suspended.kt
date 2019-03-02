package org.pgexposed.sql.transactions.experimental

import kotlinx.coroutines.runBlocking
import org.pgexposed.sql.Database
import org.pgexposed.sql.Transaction
import org.pgexposed.sql.transactions.TransactionManager

suspend fun <T> transaction(db: Database? = null, statement: suspend Transaction.() -> T): T =
        transaction(TransactionManager.manager.defaultIsolationLevel, TransactionManager.manager.defaultRepetitionAttempts, db, statement)

suspend fun <T> transaction(transactionIsolation: Int, repetitionAttempts: Int, db: Database? = null, statement: suspend Transaction.() -> T): T =
    org.pgexposed.sql.transactions.transaction(transactionIsolation, repetitionAttempts, db) {
        runBlocking { statement() }
    }
