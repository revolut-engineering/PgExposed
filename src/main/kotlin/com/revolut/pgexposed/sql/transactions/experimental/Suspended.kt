package com.revolut.pgexposed.sql.transactions.experimental

import kotlinx.coroutines.runBlocking
import com.revolut.pgexposed.sql.Database
import com.revolut.pgexposed.sql.Transaction
import com.revolut.pgexposed.sql.transactions.TransactionManager

suspend fun <T> transaction(db: Database? = null, statement: suspend Transaction.() -> T): T =
        transaction(TransactionManager.manager.defaultIsolationLevel, TransactionManager.manager.defaultRepetitionAttempts, db, statement)

suspend fun <T> transaction(transactionIsolation: Int, repetitionAttempts: Int, db: Database? = null, statement: suspend Transaction.() -> T): T =
    com.revolut.pgexposed.sql.transactions.transaction(transactionIsolation, repetitionAttempts, db) {
        runBlocking { statement() }
    }
