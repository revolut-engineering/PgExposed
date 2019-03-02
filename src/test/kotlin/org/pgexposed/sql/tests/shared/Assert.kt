package org.pgexposed.sql.tests.shared

import org.pgexposed.sql.Transaction
import org.pgexposed.sql.vendors.currentDialect
import org.pgexposed.sql.vendors.currentDialectIfAvailable
import org.joda.time.DateTime
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.fail

private fun<T> assertEqualCollectionsImpl(collection : Collection<T>, expected : Collection<T>) {
    assertEquals (expected.size, collection.size, "Count mismatch on ${currentDialect.name}")
    for (p in collection) {
        assert(expected.any {p == it}) { "Unexpected element in collection pair $p on ${currentDialect.name}" }
    }
}

fun<T> assertEqualCollections (collection : Collection<T>, expected : Collection<T>) {
    assertEqualCollectionsImpl(collection, expected)
}

fun<T> assertEqualCollections (collection : Collection<T>, vararg expected : T) {
    assertEqualCollectionsImpl(collection, expected.toList())
}

fun<T> assertEqualCollections (collection : Iterable<T>, vararg expected : T) {
    assertEqualCollectionsImpl(collection.toList(), expected.toList())
}

fun<T> assertEqualCollections (collection : Iterable<T>, expected : Collection<T>) {
    assertEqualCollectionsImpl(collection.toList(), expected)
}

fun<T> assertEqualLists (l1: List<T>, l2: List<T>) {
    assertEquals(l1.size, l2.size, "Count mismatch on ${currentDialectIfAvailable?.name.orEmpty()}")
    for (i in 0 until l1.size)
        assertEquals(l1[i], l2[i], "Error at pos $i on ${currentDialectIfAvailable?.name.orEmpty()}:")
}

fun<T> assertEqualLists (l1: List<T>, vararg expected : T) {
    assertEqualLists(l1, expected.toList())
}

fun assertEqualDateTime(d1: DateTime?, d2: DateTime?) {
    when{
        d1 == null && d2 == null -> return
        d1 == null && d2 != null -> error("d1 is null while d2 is not on ${currentDialect.name}")
        d2 == null -> error ("d1 is not null while d2 is null on ${currentDialect.name}")
        d1 == null -> error("Impossible")
        else -> assertEquals(d1.millis, d2.millis,   "Failed on ${currentDialect.name}")
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

fun equalDateTime(d1: DateTime?, d2: DateTime?) = try {
    assertEqualDateTime(d1, d2)
    true
} catch (e: Exception) {
    false
}

inline fun <reified T:Exception> expectException(body: () -> Unit) {
    try {
        body()
        fail("${T::class.simpleName} expected.")
    } catch (e: Exception) {
        if (e !is T) fail("Expected ${T::class.simpleName} but ${e::class.simpleName} thrown.")
    }
}