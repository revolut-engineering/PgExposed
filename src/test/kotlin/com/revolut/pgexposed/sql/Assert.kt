package com.revolut.pgexposed.sql

import com.revolut.pgexposed.sql.Transaction
import com.revolut.pgexposed.sql.postgres.currentDialect
import com.revolut.pgexposed.sql.postgres.currentDialectIfAvailable
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.fail

fun<T> assertEqualLists (l1: List<T>, l2: List<T>) {
    assertEquals(l1.size, l2.size, "Count mismatch on ${currentDialectIfAvailable?.name.orEmpty()}")
    for (i in 0 until l1.size)
        assertEquals(l1[i], l2[i], "Error at pos $i on ${currentDialectIfAvailable?.name.orEmpty()}:")
}

fun assertEqualDateTime(d1: LocalDateTime?, d2: LocalDateTime?) {
    when{
        d1 == null && d2 == null -> return
        d1 == null && d2 != null -> error("d1 is null while d2 is not on ${currentDialect.name}")
        d2 == null -> error ("d1 is not null while d2 is null on ${currentDialect.name}")
        d1 == null -> error("Impossible")
        else -> assertEquals(d1.nano, d2.nano,   "Failed on ${currentDialect.name}")
    }
}

fun Transaction.assertFailAndRollback(message: kotlin.String, block: () -> Unit) {
    commit()
    assertFails("Failed on ${currentDialect.name}. $message") {
        block()
        commit()
    }

    rollback()
}

inline fun <reified T:Exception> expectException(body: () -> Unit) {
    try {
        body()
        fail("${T::class.simpleName} expected.")
    } catch (e: Exception) {
        if (e !is T) fail("Expected ${T::class.simpleName} but ${e::class.simpleName} thrown.")
    }
}