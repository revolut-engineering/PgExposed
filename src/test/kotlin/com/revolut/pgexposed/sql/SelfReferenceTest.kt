package com.revolut.pgexposed.sql

import com.revolut.pgexposed.sql.tables.DMLTestsData
import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Suppress("unused", "ClassName")
class SortByReferenceTest {

    @Test
    fun simpleTest() {
        assertEqualLists(listOf(DMLTestsData.Cities), SchemaUtils.sortTablesByReferences(listOf(DMLTestsData.Cities)))
        assertEqualLists(listOf(DMLTestsData.Cities, DMLTestsData.Users), SchemaUtils.sortTablesByReferences(listOf(DMLTestsData.Users)))

        val rightOrder = listOf(DMLTestsData.Cities, DMLTestsData.Users, DMLTestsData.UserData)
        val r1 = SchemaUtils.sortTablesByReferences(listOf(DMLTestsData.Cities, DMLTestsData.UserData, DMLTestsData.Users))
        val r2 = SchemaUtils.sortTablesByReferences(listOf(DMLTestsData.UserData, DMLTestsData.Cities, DMLTestsData.Users))
        val r3 = SchemaUtils.sortTablesByReferences(listOf(DMLTestsData.Users, DMLTestsData.Cities, DMLTestsData.UserData))
        assertEqualLists(rightOrder, r1)
        assertEqualLists(rightOrder, r2)
        assertEqualLists(rightOrder, r3)
    }

    object TestTables {
        object cities : Table() {
            val id = integer("id").autoIncrement().primaryKey()
            val name = varchar("name", 50)
            val strange_id = varchar("strange_id", 10).references(TestTables.strangeTable.id)
        }

        object users : Table() {
            val id = varchar("id", 10).primaryKey()
            val name = varchar("name", length = 50)
            val cityId = (integer("city_id") references TestTables.cities.id).nullable()
        }

        object noRefereeTable : Table() {
            val id = varchar("id", 10).primaryKey()
            val col1 = varchar("col1", 10)
        }

        object refereeTable : Table() {
            val id = varchar("id", 10).primaryKey()
            val ref = (varchar("ref", 10) references TestTables.noRefereeTable.id)
        }

        object referencedTable : Table() {
            val id = varchar("id", 10).primaryKey()
            val col3 = varchar("col3", 10)
        }

        object strangeTable : Table() {
            val id = varchar("id", 10).primaryKey()
            val user_id = varchar("user_id", 10) references TestTables.users.id
            val comment = varchar("comment", 30)
            val value = integer("value")
        }
    }

    @Test
    fun cycleReferencesCheckTest() {
        val original = listOf(TestTables.cities, TestTables.users, TestTables.strangeTable, TestTables.noRefereeTable, TestTables.refereeTable, TestTables.referencedTable)
        val sortedTables = SchemaUtils.sortTablesByReferences(original)
        val expected = listOf(TestTables.users, TestTables.strangeTable, TestTables.cities, TestTables.noRefereeTable, TestTables.refereeTable, TestTables.referencedTable)

        assertEqualLists(expected, sortedTables)
    }

    @Test
    fun testHasCycle() {
        assertFalse(SchemaUtils.checkCycle(TestTables.referencedTable))
        assertFalse(SchemaUtils.checkCycle(TestTables.refereeTable))
        assertFalse(SchemaUtils.checkCycle(TestTables.noRefereeTable))
        assertTrue(SchemaUtils.checkCycle(TestTables.users))
        assertTrue(SchemaUtils.checkCycle(TestTables.cities))
        assertTrue(SchemaUtils.checkCycle(TestTables.strangeTable))

    }
}