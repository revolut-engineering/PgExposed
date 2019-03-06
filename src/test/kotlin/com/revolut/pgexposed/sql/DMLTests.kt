package com.revolut.pgexposed.sql

import com.revolut.pgexposed.exceptions.UnsupportedByDialectException
import com.revolut.pgexposed.sql.tables.DMLTestsData.BIG_DECIMAL_VALUE
import com.revolut.pgexposed.sql.tables.DMLTestsData.ST_PETERSBURG
import com.revolut.pgexposed.sql.tables.DMLTestsData.today
import com.revolut.pgexposed.sql.postgres.DatabaseDialect
import com.revolut.pgexposed.sql.postgres.PostgreSQLDialect
import com.revolut.pgexposed.sql.tables.DMLTestsData
import com.revolut.pgexposed.sql.tables.OrgMemberships
import com.revolut.pgexposed.sql.tables.Orgs
import com.revolut.pgexposed.sql.transactions.TransactionManager
import com.revolut.pgexposed.sql.transactions.transaction
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.not
import org.junit.Assert.assertThat
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.reflect.KClass
import kotlin.test.*

@Suppress("unused", "LocalVariableName")
class DMLTests : DatabaseTestsBase() {
    private fun withCitiesAndUsers(statement: Transaction.(cities: DMLTestsData.Cities, users: DMLTestsData.Users, userData: DMLTestsData.UserData) -> Unit) {
        val Users = DMLTestsData.Users
        val Cities = DMLTestsData.Cities
        val UserData = DMLTestsData.UserData

        withTables(Cities, Users, UserData) {
            val saintPetersburgId = Cities.insert {
                it[name] = ST_PETERSBURG
            } get DMLTestsData.Cities.id

            val munichId = Cities.insert {
                it[name] = "Munich"
            } get DMLTestsData.Cities.id

            Cities.insert {
                it[name] = "Prague"
            }

            Users.insert {
                it[id] = "andrey"
                it[name] = "Andrey"
                it[cityId] = saintPetersburgId
            }

            Users.insert {
                it[id] = "sergey"
                it[name] = "Sergey"
                it[cityId] = munichId
            }

            Users.insert {
                it[id] = "eugene"
                it[name] = "Eugene"
                it[cityId] = munichId
            }

            Users.insert {
                it[id] = "alex"
                it[name] = "Alex"
                it[cityId] = null
            }

            Users.insert {
                it[id] = "smth"
                it[name] = "Something"
                it[cityId] = null
            }

            UserData.insert {
                it[user_id] = "smth"
                it[comment] = "Something is here"
                it[value] = 10
            }

            UserData.insert {
                it[user_id] = "smth"
                it[comment] = "Comment #2"
                it[value] = 20
            }

            UserData.insert {
                it[user_id] = "eugene"
                it[comment] = "Comment for Eugene"
                it[value] = 20
            }

            UserData.insert {
                it[user_id] = "sergey"
                it[comment] = "Comment for Sergey"
                it[value] = 30
            }

            statement(Cities, Users, UserData)
        }
    }

    @Test
    fun testUpdate01() {
        withCitiesAndUsers { _, users, _ ->
            val alexId = "alex"
            val alexName = users.slice(DMLTestsData.Users.name).select { DMLTestsData.Users.id.eq(alexId) }.first()[DMLTestsData.Users.name]
            assertEquals("Alex", alexName)

            val newName = "Alexey"
            users.update({ DMLTestsData.Users.id.eq(alexId) }) {
                it[name] = newName
            }

            val alexNewName = users.slice(DMLTestsData.Users.name).select { DMLTestsData.Users.id.eq(alexId) }.first()[DMLTestsData.Users.name]
            assertEquals(newName, alexNewName)
        }
    }

    @Test
    fun testPreparedStatement() {
        withCitiesAndUsers { _, users, _ ->
            val name = users.select { DMLTestsData.Users.id eq "eugene" }.first()[DMLTestsData.Users.name]
            assertEquals("Eugene", name)
        }
    }

    @Test
    fun testDelete01() {
        val pattern = "%thing"

        withCitiesAndUsers { _, users, userData ->
            userData.deleteAll()
            val userDataExists = userData.selectAll().any()
            assertEquals(false, userDataExists)

            val smthId = users.slice(DMLTestsData.Users.id).select { DMLTestsData.Users.name.like(pattern) }.single()[DMLTestsData.Users.id]
            assertEquals("smth", smthId)

            users.deleteWhere { DMLTestsData.Users.name like pattern }
            val hasSmth = users.slice(DMLTestsData.Users.id).select { DMLTestsData.Users.name.like(pattern) }.any()
            assertEquals(false, hasSmth)
        }
    }

    // select expressions
    @Test
    fun testSelect() {
        withCitiesAndUsers { _, users, _ ->
            users.select { DMLTestsData.Users.id.eq("andrey") }.forEach {
                val userId = it[DMLTestsData.Users.id]
                val userName = it[DMLTestsData.Users.name]
                when (userId) {
                    "andrey" -> assertEquals("Andrey", userName)
                    else -> error("Unexpected user $userId")
                }
            }
        }
    }

    @Test
    fun testSelectAnd() {
        withCitiesAndUsers { _, users, _ ->
            users.select { DMLTestsData.Users.id.eq("andrey") and DMLTestsData.Users.name.eq("Andrey") }.forEach {
                val userId = it[DMLTestsData.Users.id]
                val userName = it[DMLTestsData.Users.name]
                when (userId) {
                    "andrey" -> assertEquals("Andrey", userName)
                    else -> error("Unexpected user $userId")
                }
            }
        }
    }

    @Test
    fun testSelectOr() {
        withCitiesAndUsers { _, users, _ ->
            users.select { DMLTestsData.Users.id.eq("andrey") or DMLTestsData.Users.name.eq("Andrey") }.forEach {
                val userId = it[DMLTestsData.Users.id]
                val userName = it[DMLTestsData.Users.name]
                when (userId) {
                    "andrey" -> assertEquals("Andrey", userName)
                    else -> error("Unexpected user $userId")
                }
            }
        }
    }

    @Test
    fun testSelectNot() {
        withCitiesAndUsers { _, users, _ ->
            users.select { com.revolut.pgexposed.sql.not(DMLTestsData.Users.id.eq("andrey")) }.forEach {
                val userId = it[DMLTestsData.Users.id]
                if (userId == "andrey") {
                    error("Unexpected user $userId")
                }
            }
        }
    }

    // manual join
    @Test
    fun testJoin01() {
        withCitiesAndUsers { cities, users, _ ->
            (users innerJoin cities).slice(DMLTestsData.Users.name, DMLTestsData.Cities.name).select { (DMLTestsData.Users.id.eq("andrey") or DMLTestsData.Users.name.eq("Sergey")) and DMLTestsData.Users.cityId.eq(DMLTestsData.Cities.id) }.forEach {
                val userName = it[DMLTestsData.Users.name]
                val cityName = it[DMLTestsData.Cities.name]
                when (userName) {
                    "Andrey" -> assertEquals(ST_PETERSBURG, cityName)
                    "Sergey" -> assertEquals("Munich", cityName)
                    else -> error("Unexpected user $userName")
                }
            }
        }
    }

    // join with foreign key
    @Test
    fun testJoin02() {
        withCitiesAndUsers { cities, users, _ ->
            val stPetersburgUser = (users innerJoin cities).slice(DMLTestsData.Users.name, DMLTestsData.Users.cityId, DMLTestsData.Cities.name).select { DMLTestsData.Cities.name.eq(ST_PETERSBURG) or DMLTestsData.Users.cityId.isNull() }.single()
            assertEquals("Andrey", stPetersburgUser[DMLTestsData.Users.name])
            assertEquals(ST_PETERSBURG, stPetersburgUser[DMLTestsData.Cities.name])
        }
    }

    // triple join
    @Test
    fun testJoin03() {
        withCitiesAndUsers { cities, users, userData ->
            val r = (cities innerJoin users innerJoin userData).selectAll().orderBy(DMLTestsData.Users.id).toList()
            assertEquals(2, r.size)
            assertEquals("Eugene", r[0][DMLTestsData.Users.name])
            assertEquals("Comment for Eugene", r[0][DMLTestsData.UserData.comment])
            assertEquals("Munich", r[0][DMLTestsData.Cities.name])
            assertEquals("Sergey", r[1][DMLTestsData.Users.name])
            assertEquals("Comment for Sergey", r[1][DMLTestsData.UserData.comment])
            assertEquals("Munich", r[1][DMLTestsData.Cities.name])
        }
    }

    // triple join
    @Test
    fun testJoin04() {
        val Numbers = object : Table("Numbers") {
            val id = integer("id").primaryKey()
        }

        val Names = object : Table("Names") {
            val name = varchar("name", 10).primaryKey()
        }

        val Map = object : Table("Map") {
            val id_ref = integer("id_ref") references Numbers.id
            val name_ref = varchar("name_ref", 10) references Names.name
        }

        withTables(Numbers, Names, Map) {
            Numbers.insert { it[id] = 1 }
            Numbers.insert { it[id] = 2 }
            Names.insert { it[name] = "Foo" }
            Names.insert { it[name] = "Bar" }
            Map.insert {
                it[id_ref] = 2
                it[name_ref] = "Foo"
            }

            val r = (Numbers innerJoin Map innerJoin Names).selectAll().toList()
            assertEquals(1, r.size)
            assertEquals(2, r[0][Numbers.id])
            assertEquals("Foo", r[0][Names.name])
        }
    }

    // cross join
    @Test
    fun testJoin05() {
        withCitiesAndUsers { cities, users, _ ->
            val allUsersToStPetersburg = (users crossJoin cities).slice(DMLTestsData.Users.name, DMLTestsData.Users.cityId, DMLTestsData.Cities.name).select { DMLTestsData.Cities.name.eq(ST_PETERSBURG) }.map {
                it[DMLTestsData.Users.name] to it[DMLTestsData.Cities.name]
            }
            val allUsers = setOf(
                "Andrey",
                "Sergey",
                "Eugene",
                "Alex",
                "Something"
            )
            assertTrue(allUsersToStPetersburg.all { it.second == ST_PETERSBURG })
            assertEquals(allUsers, allUsersToStPetersburg.map { it.first }.toSet())
        }
    }

    @Test
    fun testMultipleReferenceJoin01() {
        val foo = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val baz = integer("baz").uniqueIndex()
        }
        val bar = object : Table("bar") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = (integer("foo") references foo.id)
            val baz = integer("baz") references foo.baz
        }
        withTables(foo, bar) {
            val resultSet = foo.insert {
                it[baz] = 5
            }

            bar.insert {
                it[bar.foo] = resultSet[foo.id]!!
                it[baz] = 5
            }

            val result = foo.innerJoin(bar).selectAll()
            assertEquals(1, result.count())
        }
    }

    @Test
    fun testMultipleReferenceJoin02() {
        val foo = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val baz = integer("baz").uniqueIndex()
        }
        val bar = object : Table("bar") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = (integer("foo") references foo.id)
            val foo2 = (integer("foo2") references foo.id)
            val baz = integer("baz") references foo.baz
        }
        withTables(foo, bar) {
            expectException<IllegalStateException> {
                val resultSet = foo.insert {
                    it[baz] = 5
                }

                bar.insert {
                    it[bar.foo] = resultSet[foo.id]!!
                    it[bar.foo2] = resultSet[foo.id]!!
                    it[baz] = 5
                }

                val result = foo.innerJoin(bar).selectAll()
                assertEquals(1, result.count())
            }
        }
    }

    @Test
    fun testGroupBy01() {
        withCitiesAndUsers { cities, users, _ ->
            val cAlias = DMLTestsData.Users.id.count().alias("c")
            ((cities innerJoin users).slice(DMLTestsData.Cities.name, DMLTestsData.Users.id.count(), cAlias).selectAll().groupBy(DMLTestsData.Cities.name)).forEach {
                val cityName = it[DMLTestsData.Cities.name]
                val userCount = it[DMLTestsData.Users.id.count()]
                val userCountAlias = it[cAlias]

                when (cityName) {
                    "Munich" -> {
                        assertEquals(2, userCount)
                        assertEquals(2, userCountAlias)
                    }
                    "Prague" -> {
                        assertEquals(0, userCount)
                        assertEquals(0, userCountAlias)
                    }
                    ST_PETERSBURG -> {
                        assertEquals(1, userCount)
                        assertEquals(1, userCountAlias)
                    }
                    else -> error("Unknow city $cityName")
                }
            }
        }
    }

    @Test
    fun testGroupBy02() {
        withCitiesAndUsers { cities, users, _ ->
            val r = (cities innerJoin users).slice(DMLTestsData.Cities.name, DMLTestsData.Users.id.count()).selectAll().groupBy(DMLTestsData.Cities.name).having { DMLTestsData.Users.id.count() eq 1 }.toList()
            assertEquals(1, r.size)
            assertEquals(ST_PETERSBURG, r[0][DMLTestsData.Cities.name])
            val count = r[0][DMLTestsData.Users.id.count()]
            assertEquals(1, count)
        }
    }

    @Test
    fun testGroupBy03() {
        withCitiesAndUsers { cities, users, _ ->
            val maxExpr = DMLTestsData.Cities.id.max()
            val r = (cities innerJoin users).slice(DMLTestsData.Cities.name, DMLTestsData.Users.id.count(), maxExpr).selectAll()
                    .groupBy(DMLTestsData.Cities.name)
                    .having{ DMLTestsData.Users.id.count().eq(maxExpr)}
                    .orderBy(DMLTestsData.Cities.name)
                    .toList()

            assertEquals(2, r.size)
            0.let {
                assertEquals("Munich", r[it][DMLTestsData.Cities.name])
                val count = r[it][DMLTestsData.Users.id.count()]
                assertEquals(2, count)
                val max = r[it][maxExpr]
                assertEquals(2, max)
            }
            1.let {
                assertEquals(ST_PETERSBURG, r[it][DMLTestsData.Cities.name])
                val count = r[it][DMLTestsData.Users.id.count()]
                assertEquals(1, count)
                val max = r[it][maxExpr]
                assertEquals(1, max)
            }
        }
    }

    @Test
    fun testGroupBy04() {
        withCitiesAndUsers { cities, users, _ ->
            val r = (cities innerJoin users).slice(DMLTestsData.Cities.name, DMLTestsData.Users.id.count(), DMLTestsData.Cities.id.max()).selectAll()
                .groupBy(DMLTestsData.Cities.name)
                .having { DMLTestsData.Users.id.count() lessEq 42 }
                .orderBy(DMLTestsData.Cities.name)
                .toList()

            assertEquals(2, r.size)
            0.let {
                assertEquals("Munich", r[it][DMLTestsData.Cities.name])
                val count = r[it][DMLTestsData.Users.id.count()]
                assertEquals(2, count)
            }
            1.let {
                assertEquals(ST_PETERSBURG, r[it][DMLTestsData.Cities.name])
                val count = r[it][DMLTestsData.Users.id.count()]
                assertEquals(1, count)
            }
        }
    }

    @Test
    fun testGroupBy05() {
        withCitiesAndUsers { _, users, _ ->
            val maxNullableCityId = DMLTestsData.Users.cityId.max()

            users.slice(maxNullableCityId).selectAll()
                .map { it[maxNullableCityId] }.let { result ->
                assertEquals(result.size, 1)
                assertNotNull(result.single())
            }

            users.slice(maxNullableCityId).select { DMLTestsData.Users.cityId.isNull() }
                .map { it[maxNullableCityId] }.let { result ->
                assertEquals(result.size, 1)
                assertNull(result.single())
            }
        }
    }

    @Test
    fun testGroupBy06() {
        withCitiesAndUsers { cities, _, _ ->
            val maxNullableId = DMLTestsData.Cities.id.max()

            cities.slice(maxNullableId).selectAll()
                .map { it[maxNullableId] }.let { result ->
                assertEquals(result.size, 1)
                assertNotNull(result.single())
            }

            cities.slice(maxNullableId).select { DMLTestsData.Cities.id.isNull() }
                .map { it[maxNullableId] }.let { result: List<Int?> ->
                assertEquals(result.size, 1)
                assertNull(result.single())
            }
        }
    }

    @Test
    fun testGroupBy07() {
        withCitiesAndUsers { cities, _, _ ->
            val avgIdExpr = DMLTestsData.Cities.id.avg()
            val avgId = BigDecimal.valueOf(cities.selectAll().map { it[DMLTestsData.Cities.id] }.average())

            cities.slice(avgIdExpr).selectAll()
                .map { it[avgIdExpr] }.let { result ->
                assertEquals(result.size, 1)
                        assertEquals(result.single()!!.compareTo(avgId), 0)
            }

            cities.slice(avgIdExpr).select { DMLTestsData.Cities.id.isNull() }
                .map { it[avgIdExpr] }.let { result ->
                assertEquals(result.size, 1)
                assertNull(result.single())
            }
        }
    }

    @Test
    fun testGroupConcat() {
        withCitiesAndUsers { cities, users, _ ->
            fun <T : String?> GroupConcat<T>.checkExcept(vararg dialects: KClass<out DatabaseDialect>, assert: (Map<String, String?>) ->Unit) {
                try {
                    val result = cities.leftJoin(users)
                        .slice(DMLTestsData.Cities.name, this)
                        .selectAll()
                        .groupBy(DMLTestsData.Cities.id, DMLTestsData.Cities.name).associate {
                            it[DMLTestsData.Cities.name] to it[this]
                        }
                    assert(result)
                } catch (e: UnsupportedByDialectException) {
                    assertTrue(e.dialect::class in dialects, e.message!! )
                }
            }

            DMLTestsData.Users.name.groupConcat(separator = ", ").checkExcept {
                assertEquals(3, it.size)
                assertEquals("Andrey", it[ST_PETERSBURG])
                val sorted = "Sergey, Eugene"
                assertEquals(sorted, it["Munich"])
                assertNull(it["Prague"])
            }

            DMLTestsData.Users.name.groupConcat(separator = " | ", distinct = true).checkExcept(PostgreSQLDialect::class) {
                assertEquals(3, it.size)
                assertEquals("Andrey", it[ST_PETERSBURG])
                val sorted = "Sergey | Eugene"
                assertEquals(sorted, it["Munich"])
                assertNull(it["Prague"])
            }

            DMLTestsData.Users.name.groupConcat(separator = " | ", orderBy = DMLTestsData.Users.name to SortOrder.ASC).checkExcept(PostgreSQLDialect::class) {
                assertEquals(3, it.size)
                assertEquals("Andrey", it[ST_PETERSBURG])
                assertEquals("Eugene | Sergey", it["Munich"])
                assertNull(it["Prague"])
            }

            DMLTestsData.Users.name.groupConcat(separator = " | ", orderBy = DMLTestsData.Users.name to SortOrder.DESC).checkExcept(PostgreSQLDialect::class) {
                assertEquals(3, it.size)
                assertEquals("Andrey", it[ST_PETERSBURG])
                assertEquals("Sergey | Eugene", it["Munich"])
                assertNull(it["Prague"])
            }
        }
    }

    @Test
    fun orderBy01() {
        withCitiesAndUsers { _, users, _ ->
            val r = users.selectAll().orderBy(DMLTestsData.Users.id).toList()
            assertEquals(5, r.size)
            assertEquals("alex", r[0][DMLTestsData.Users.id])
            assertEquals("andrey", r[1][DMLTestsData.Users.id])
            assertEquals("eugene", r[2][DMLTestsData.Users.id])
            assertEquals("sergey", r[3][DMLTestsData.Users.id])
            assertEquals("smth", r[4][DMLTestsData.Users.id])
        }
    }

    @Test
    fun orderBy02() {
        withCitiesAndUsers { _, users, _ ->
            val r = users.selectAll().orderBy(DMLTestsData.Users.cityId, SortOrder.DESC).orderBy(DMLTestsData.Users.id).toList()
            assertEquals(5, r.size)
            val usersWithoutCities = listOf("alex", "smth")
            val otherUsers = listOf("eugene", "sergey", "andrey")
            val expected = usersWithoutCities + otherUsers
            expected.forEachIndexed { index, e ->
                assertEquals(e, r[index][DMLTestsData.Users.id])
            }
        }
    }

    @Test
    fun orderBy03() {
        withCitiesAndUsers { _, users, _ ->
            val r = users.selectAll().orderBy(DMLTestsData.Users.cityId to SortOrder.DESC, DMLTestsData.Users.id to SortOrder.ASC).toList()
            assertEquals(5, r.size)
            val usersWithoutCities = listOf("alex", "smth")
            val otherUsers = listOf("eugene", "sergey", "andrey")
            val expected = usersWithoutCities + otherUsers
            expected.forEachIndexed { index, e ->
                assertEquals(e, r[index][DMLTestsData.Users.id])
            }
        }
    }

    @Test
    fun testOrderBy04() {
        withCitiesAndUsers { cities, users, _ ->
            val r = (cities innerJoin users).slice(DMLTestsData.Cities.name, DMLTestsData.Users.id.count()).selectAll().groupBy(DMLTestsData.Cities.name).orderBy(DMLTestsData.Cities.name).toList()
            assertEquals(2, r.size)
            assertEquals("Munich", r[0][DMLTestsData.Cities.name])
            assertEquals(2, r[0][DMLTestsData.Users.id.count()])
            assertEquals(ST_PETERSBURG, r[1][DMLTestsData.Cities.name])
            assertEquals(1, r[1][DMLTestsData.Users.id.count()])
        }
    }

    @Test
    fun orderBy05() {
        withCitiesAndUsers { _, users, _ ->
            val r = users.selectAll().orderBy(DMLTestsData.Users.cityId to SortOrder.DESC, DMLTestsData.Users.id to SortOrder.ASC).toList()
            assertEquals(5, r.size)
            val usersWithoutCities = listOf("alex", "smth")
            val otherUsers = listOf("eugene", "sergey", "andrey")
            val expected = usersWithoutCities + otherUsers
            expected.forEachIndexed { index, e ->
                assertEquals(e, r[index][DMLTestsData.Users.id])
            }
        }
    }

    @Test
    fun orderBy06() {
        withCitiesAndUsers { _, users, _ ->
            val orderByExpression = DMLTestsData.Users.id.substring(2, 1)
            val r = users.selectAll().orderBy(orderByExpression to SortOrder.ASC).toList()
            assertEquals(5, r.size)
            assertEquals("sergey", r[0][DMLTestsData.Users.id])
            assertEquals("alex", r[1][DMLTestsData.Users.id])
            assertEquals("smth", r[2][DMLTestsData.Users.id])
            assertEquals("andrey", r[3][DMLTestsData.Users.id])
            assertEquals("eugene", r[4][DMLTestsData.Users.id])
        }
    }

    @Test
    fun testSizedIterable() {
        withCitiesAndUsers { cities, _, _ ->
            assertEquals(false, cities.selectAll().empty())
            assertEquals(true, cities.select { DMLTestsData.Cities.name eq "Qwertt" }.empty())
            assertEquals(0, cities.select { DMLTestsData.Cities.name eq "Qwertt" }.count())
            assertEquals(3, cities.selectAll().count())
        }
    }

    @Test
    fun testExists01() {
        withCitiesAndUsers { _, users, userData ->
            val r = users.select { exists(userData.select((DMLTestsData.UserData.user_id eq DMLTestsData.Users.id) and (DMLTestsData.UserData.comment like "%here%"))) }.toList()
            assertEquals(1, r.size)
            assertEquals("Something", r[0][DMLTestsData.Users.name])
        }
    }

    @Test
    fun testExists02() {
        withCitiesAndUsers { _, users, userData ->
            val r = users.select { exists(userData.select((DMLTestsData.UserData.user_id eq DMLTestsData.Users.id) and ((DMLTestsData.UserData.comment like "%here%") or (DMLTestsData.UserData.comment like "%Sergey")))) }
                .orderBy(DMLTestsData.Users.id).toList()
            assertEquals(2, r.size)
            assertEquals("Sergey", r[0][DMLTestsData.Users.name])
            assertEquals("Something", r[1][DMLTestsData.Users.name])
        }
    }

    @Test
    fun testExists03() {
        withCitiesAndUsers { _, users, userData ->
            val r = users.select {
                exists(userData.select((DMLTestsData.UserData.user_id eq DMLTestsData.Users.id) and (DMLTestsData.UserData.comment like "%here%"))) or
                    exists(userData.select((DMLTestsData.UserData.user_id eq DMLTestsData.Users.id) and (DMLTestsData.UserData.comment like "%Sergey")))
            }
                .orderBy(DMLTestsData.Users.id).toList()
            assertEquals(2, r.size)
            assertEquals("Sergey", r[0][DMLTestsData.Users.name])
            assertEquals("Something", r[1][DMLTestsData.Users.name])
        }
    }

    @Test
    fun testInList01() {
        withCitiesAndUsers { _, users, _ ->
            val r = users.select { DMLTestsData.Users.id inList listOf("andrey", "alex") }.orderBy(DMLTestsData.Users.name).toList()

            assertEquals(2, r.size)
            assertEquals("Alex", r[0][DMLTestsData.Users.name])
            assertEquals("Andrey", r[1][DMLTestsData.Users.name])
        }
    }

    @Test
    fun testInList02() {
        withCitiesAndUsers { cities, _, _ ->
            val cityIds = cities.selectAll().map { it[DMLTestsData.Cities.id] }.take(2)
            val r = cities.select { DMLTestsData.Cities.id inList cityIds }

            assertEquals(2, r.count())
        }
    }

    @Test
    fun testCalc01() {
        withCitiesAndUsers { cities, _, _ ->
            val r = cities.slice(DMLTestsData.Cities.id.sum()).selectAll().toList()
            assertEquals(1, r.size)
            assertEquals(6, r[0][DMLTestsData.Cities.id.sum()])
        }
    }

    @Test
    fun testCalc02() {
        withCitiesAndUsers { cities, users, userData ->
            val sum = Expression.build {
                Sum(DMLTestsData.Cities.id + DMLTestsData.UserData.value, IntegerColumnType())
            }
            val r = (users innerJoin userData innerJoin cities).slice(DMLTestsData.Users.id, sum)
                .selectAll().groupBy(DMLTestsData.Users.id).orderBy(DMLTestsData.Users.id).toList()
            assertEquals(2, r.size)
            assertEquals("eugene", r[0][DMLTestsData.Users.id])
            assertEquals(22, r[0][sum])
            assertEquals("sergey", r[1][DMLTestsData.Users.id])
            assertEquals(32, r[1][sum])
        }
    }

    @Test
    fun testCalc03() {
        withCitiesAndUsers { cities, users, userData ->
            val sum = Expression.build { Sum(DMLTestsData.Cities.id * 100 + DMLTestsData.UserData.value / 10, IntegerColumnType()) }
            val mod1 = Expression.build { sum % 100 }
            val mod2 = Expression.build { sum mod 100 }
            val r = (users innerJoin userData innerJoin cities).slice(DMLTestsData.Users.id, sum, mod1, mod1)
                .selectAll().groupBy(DMLTestsData.Users.id).orderBy(DMLTestsData.Users.id).toList()
            assertEquals(2, r.size)
            assertEquals("eugene", r[0][DMLTestsData.Users.id])
            assertEquals(202, r[0][sum])
            assertEquals(2, r[0][mod1])
            assertEquals(2, r[0][mod2])
            assertEquals("sergey", r[1][DMLTestsData.Users.id])
            assertEquals(203, r[1][sum])
            assertEquals(3, r[1][mod1])
            assertEquals(3, r[1][mod2])
        }
    }

    @Test
    fun testSubstring01() {
        withCitiesAndUsers { _, users, _ ->
            val substring = DMLTestsData.Users.name.substring(1, 2)
            val r = (users).slice(DMLTestsData.Users.id, substring)
                .selectAll().orderBy(DMLTestsData.Users.id).toList()
            assertEquals(5, r.size)
            assertEquals("Al", r[0][substring])
            assertEquals("An", r[1][substring])
            assertEquals("Eu", r[2][substring])
            assertEquals("Se", r[3][substring])
            assertEquals("So", r[4][substring])
        }
    }

    @Test
    fun testLengthWithCount01() {
        class LengthFunction<T: ExpressionWithColumnType<String>>(val exp: T) : Function<Int>(IntegerColumnType()) {
            override fun toSQL(queryBuilder: QueryBuilder): String
                = "LENGTH(${exp.toSQL(queryBuilder)})"
        }
        withCitiesAndUsers { cities, _, _ ->
            val sumOfLength = LengthFunction(DMLTestsData.Cities.name).sum()
            val expectedValue = cities.selectAll().sumBy{ it[DMLTestsData.Cities.name].length }

            val results = cities.slice(sumOfLength).selectAll().toList()
            assertEquals(1, results.size)
            assertEquals(expectedValue, results.single()[sumOfLength])
        }
    }

    @Test
    fun testInsertSelect01() {
        withCitiesAndUsers { cities, users, _ ->
            val substring = DMLTestsData.Users.name.substring(1, 2)
            cities.insert(users.slice(substring).selectAll().orderBy(DMLTestsData.Users.id).limit(2))

            val r = cities.slice(DMLTestsData.Cities.name).selectAll().orderBy(DMLTestsData.Cities.id, SortOrder.DESC).limit(2).toList()
            assertEquals(2, r.size)
            assertEquals("An", r[0][DMLTestsData.Cities.name])
            assertEquals("Al", r[1][DMLTestsData.Cities.name])
        }
    }

    @Test
    fun testInsertSelect02() {
        withCitiesAndUsers { _, _, userData ->
            val allUserData = userData.selectAll().count()
            userData.insert(userData.slice(DMLTestsData.UserData.user_id, DMLTestsData.UserData.comment, intParam(42)).selectAll())

            val r = userData.select { DMLTestsData.UserData.value eq 42 }.orderBy(DMLTestsData.UserData.user_id).toList()
            assertEquals(allUserData, r.size)
        }
    }

    @Test
    fun testInsertSelect03() {
        withCitiesAndUsers { _, users, _ ->
            val userCount = users.selectAll().count()
            users.insert(users.slice(Random().castTo<String>(VarCharColumnType()).substring(1, 10), stringParam("Foo"), intParam(1)).selectAll())
            val r = users.select { DMLTestsData.Users.name eq "Foo" }.toList()
            assertEquals(userCount, r.size)
        }
    }

    @Test
    fun testInsertSelect04() {
        withCitiesAndUsers { _, users, _ ->
            val userCount = users.selectAll().count()
            users.insert(users.slice(stringParam("Foo"), Random().castTo<String>(VarCharColumnType()).substring(1, 10)).selectAll(), columns = listOf(DMLTestsData.Users.name, DMLTestsData.Users.id))
            val r = users.select { DMLTestsData.Users.name eq "Foo" }.toList()
            assertEquals(userCount, r.size)
        }
    }

    @Test
    fun testSelectCase01() {
        withCitiesAndUsers { _, users, _ ->
            val field = Expression.build { case().When(DMLTestsData.Users.id eq "alex", stringLiteral("11")).Else(stringLiteral("22")) }
            val r = users.slice(DMLTestsData.Users.id, field).selectAll().orderBy(DMLTestsData.Users.id).limit(2).toList()
            assertEquals(2, r.size)
            assertEquals("11", r[0][field])
            assertEquals("alex", r[0][DMLTestsData.Users.id])
            assertEquals("22", r[1][field])
            assertEquals("andrey", r[1][DMLTestsData.Users.id])
        }
    }

    private fun DMLTestsData.Misc.checkRow(row: ResultRow, n: Int, nn: Int?, d: LocalDateTime, dn: LocalDateTime?,
                                                                            t: LocalDateTime, tn: LocalDateTime?, e: DMLTestsData.E, en: DMLTestsData.E?,
                                                                            es: DMLTestsData.E, esn: DMLTestsData.E?, s: String, sn: String?,
                                                                            dc: BigDecimal, dcn: BigDecimal?, fcn: Float?, dblcn: Double?) {
        assertEquals(row[DMLTestsData.Misc.n], n)
        assertEquals(row[DMLTestsData.Misc.nn], nn)
        assertEqualDateTime(row[DMLTestsData.Misc.d], d)
        assertEqualDateTime(row[DMLTestsData.Misc.dn], dn)
        assertEqualDateTime(row[DMLTestsData.Misc.t], t)
        assertEqualDateTime(row[DMLTestsData.Misc.tn], tn)
        assertEquals(row[DMLTestsData.Misc.e], e)
        assertEquals(row[DMLTestsData.Misc.en], en)
        assertEquals(row[DMLTestsData.Misc.es], es)
        assertEquals(row[DMLTestsData.Misc.esn], esn)
        assertEquals(row[DMLTestsData.Misc.s], s)
        assertEquals(row[DMLTestsData.Misc.sn], sn)
        assertEquals(row[DMLTestsData.Misc.dc], dc)
        assertEquals(row[DMLTestsData.Misc.dcn], dcn)
        assertEquals(row[DMLTestsData.Misc.fcn], fcn)
        assertEquals(row[DMLTestsData.Misc.dblcn], dblcn)
    }

    @Test
    fun testInsert01() {
        val tbl = DMLTestsData.Misc
        val date = today
        val time = LocalDateTime.now()

        withTables(tbl) {
            tbl.insert {
                it[n] = 42
                it[d] = date
                it[t] = time
                it[e] = DMLTestsData.E.ONE
                it[es] = DMLTestsData.E.ONE
                it[s] = "test"
                it[dc] = BIG_DECIMAL_VALUE
                it[char] = '('
            }

            val row = tbl.selectAll().single()
            tbl.checkRow(row, 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE,
                    null, "test", null, BIG_DECIMAL_VALUE, null, null, null)
            assertEquals('(', row[DMLTestsData.Misc.char])
        }
    }

    @Test
    fun testInsert02() {
        val tbl = DMLTestsData.Misc
        val date = today
        val time = LocalDateTime.now()

        withTables(tbl) {
            tbl.insert {
                it[n] = 42
                it[nn] = null
                it[d] = date
                it[dn] = null
                it[t] = time
                it[tn] = null
                it[e] = DMLTestsData.E.ONE
                it[en] = null
                it[es] = DMLTestsData.E.ONE
                it[esn] = null
                it[s] = "test"
                it[sn] = null
                it[dc] = BIG_DECIMAL_VALUE
                it[dcn] = null
                it[fcn] = null
            }

            val row = tbl.selectAll().single()
            tbl.checkRow(row, 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE,
                    null, "test", null, BIG_DECIMAL_VALUE, null, null, null)
        }
    }

    @Test
    fun testInsert03() {
        val tbl = DMLTestsData.Misc
        val date = today
        val time = LocalDateTime.now()

        withTables(tbl) {
            tbl.insert {
                it[n] = 42
                it[nn] = 42
                it[d] = date
                it[dn] = date
                it[t] = time
                it[tn] = time
                it[e] = DMLTestsData.E.ONE
                it[en] = DMLTestsData.E.ONE
                it[es] = DMLTestsData.E.ONE
                it[esn] = DMLTestsData.E.ONE
                it[s] = "test"
                it[sn] = "test"
                it[dc] = BIG_DECIMAL_VALUE
                it[dcn] = BIG_DECIMAL_VALUE
                it[fcn] = 239.42f
                it[dblcn] = 567.89
            }

            val row = tbl.selectAll().single()
            tbl.checkRow(row, 42, 42, date, date, time, time, DMLTestsData.E.ONE, DMLTestsData.E.ONE, DMLTestsData.E.ONE, DMLTestsData.E.ONE,
                    "test", "test", BIG_DECIMAL_VALUE, BIG_DECIMAL_VALUE, 239.42f, 567.89)
        }
    }

    @Test
    fun testInsert04() {
        val stringThatNeedsEscaping = "A'braham Barakhyahu"
        val tbl = DMLTestsData.Misc
        val date = today
        val time = LocalDateTime.now()
        withTables(tbl) {
            tbl.insert {
                it[n] = 42
                it[d] = date
                it[t] = time
                it[e] = DMLTestsData.E.ONE
                it[es] = DMLTestsData.E.ONE
                it[s] = stringThatNeedsEscaping
                it[dc] = BIG_DECIMAL_VALUE
            }

            val row = tbl.selectAll().single()
            tbl.checkRow(row, 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, stringThatNeedsEscaping, null,
                    BIG_DECIMAL_VALUE, null, null, null)
        }
    }

    @Test
    fun testInsertAndGetId01() {
        val idTable = object : Table("tmp") {
            val id = integer("id").autoIncrement().primaryKey()
            val name = varchar("foo", 10).uniqueIndex()
        }

        withTables(idTable) {
            idTable.insert {
                it[idTable.name] = "1"
            }

            assertEquals(1, idTable.selectAll().count())

            idTable.insert {
                it[idTable.name] = "2"
            }

            assertEquals(2, idTable.selectAll().count())

            assertFailAndRollback("Unique constraint") {
                idTable.insert {
                    it[idTable.name] = "2"
                }
            }
        }
    }

    @Test
    fun testInsertIgnoreAndGetId01() {
        val idTable = object : Table("tmp") {
            val id = integer("id").autoIncrement().primaryKey()
            val name = varchar("foo", 10).uniqueIndex()
        }

        withTables(idTable) {
            idTable.insertIgnore {
                it[idTable.name] = "1"
            }

            assertEquals(1, idTable.selectAll().count())

            idTable.insertIgnore {
                it[idTable.name] = "2"
            }

            assertEquals(2, idTable.selectAll().count())

            val changes = idTable.insertIgnore {
                it[idTable.name] = "2"
            }

            assertFalse(changes.resultedValues!![0].hasValue(idTable.id))
        }
    }


    @Test
    fun testBatchInsert01() {
        withCitiesAndUsers { cities, users, _ ->
            val cityNames = listOf("Paris", "Moscow", "Helsinki")
            val allCitiesID = cities.batchInsert(cityNames) { name ->
                this[DMLTestsData.Cities.name] = name
            }
            assertEquals(cityNames.size, allCitiesID.size)

            val userNamesWithCityIds = allCitiesID.mapIndexed { index, id ->
                "UserFrom${cityNames[index]}" to id[DMLTestsData.Cities.id] as Number
            }

            val generatedIds = users.batchInsert(userNamesWithCityIds) { (userName, cityId) ->
                this[DMLTestsData.Users.id] = java.util.Random().nextInt().toString().take(6)
                this[DMLTestsData.Users.name] = userName
                this[DMLTestsData.Users.cityId] = cityId.toInt()
            }

            assertEquals(userNamesWithCityIds.size, generatedIds.size)
            assertEquals(userNamesWithCityIds.size, users.select { DMLTestsData.Users.name inList userNamesWithCityIds.map { it.first } }.count())
        }
    }

    @Test
    fun testBatchInsert02WithDefaultValueSetOnDb() {
        withDb {
            transaction {
                exec("CREATE TABLE TEST(id INT PRIMARY KEY, date timestamp NOT NULL DEFAULT clock_timestamp());")
            }
        }

        val TestTable = object: Table("TEST") {
            val id = integer("id").primaryKey()
            val date = datetime("date")
        }

        withTables(TestTable) {
            addLogger(StdOutSqlLogger)
            val ids = listOf(1..5).flatten()
            val values = TestTable.batchInsert(ids) { id ->
                this[TestTable.id] = id
            }

            assertEquals(values.size, 5)
            assertEquals(values.size, TestTable.selectAll().count())
        }
    }

    @Test
    fun testBatchInsert02IgnoringDuplicates() {
        withCitiesAndUsers { cities, users, _ ->
            addLogger(StdOutSqlLogger)
            val cityNames = listOf("Krakow", "Paris", "Moscow", "Helsinki", "Krakow")
            val allCitiesID = cities.batchInsert(cityNames, ignore = true) { name ->
                this[DMLTestsData.Cities.name] = name
            }.filter { it.hasValue(DMLTestsData.Cities.id) }

            assertEquals(cityNames.size - 1, allCitiesID.size)

            val userNamesWithCityIds = allCitiesID.mapIndexed { index, id ->
                "UserFrom${cityNames[index]}" to id[DMLTestsData.Cities.id] as Number
            }

            val generatedIds = users.batchInsert(userNamesWithCityIds) { (userName, cityId) ->
                this[DMLTestsData.Users.id] = java.util.Random().nextInt().toString().take(6)
                this[DMLTestsData.Users.name] = userName
                this[DMLTestsData.Users.cityId] = cityId.toInt()
            }

            assertEquals(userNamesWithCityIds.size, generatedIds.size)
            assertEquals(userNamesWithCityIds.size, users.select { DMLTestsData.Users.name inList userNamesWithCityIds.map { it.first } }.count())
        }
    }

    @Test
    fun testGeneratedKey01() {
        withTables(DMLTestsData.Cities) {
            val id = DMLTestsData.Cities.insert {
                it[name] = "FooCity"
            } get DMLTestsData.Cities.id
            assertEquals(DMLTestsData.Cities.selectAll().last()[DMLTestsData.Cities.id], id)
        }
    }

    object LongIdTable : Table() {
        val id = long("id").autoIncrement("long_id_seq").primaryKey()
        val name = text("name")
    }

    @Test
    fun testGeneratedKey02() {
        withTables(LongIdTable) {
            val id = LongIdTable.insert {
                it[name] = "Foo"
            } get LongIdTable.id
            assertEquals(LongIdTable.selectAll().last()[LongIdTable.id], id)
        }
    }

    @Test
    fun testSelectDistinct() {
        val tbl = object: Table("Duplicate") {
            val id = integer("id").autoIncrement("id_seq").primaryKey()
            val name = varchar("name", 50)
        }

        withTables(tbl) {
            tbl.insert { it[tbl.name] = "test" }
            tbl.insert { it[tbl.name] = "test" }

            assertEquals(2, tbl.selectAll().count())
            assertEquals(2, tbl.selectAll().withDistinct().count())
            assertEquals(1, tbl.slice(tbl.name).selectAll().withDistinct().count())
            assertEquals("test", tbl.slice(tbl.name).selectAll().withDistinct().single()[tbl.name])
        }
    }

    @Test
    fun testSelect01() {
        val tbl = DMLTestsData.Misc
        withTables(tbl) {
            val date = today
            val time = LocalDateTime.now()
            val sTest = "test"
            val dec = BIG_DECIMAL_VALUE
            tbl.insert {
                it[n] = 42
                it[d] = date
                it[t] = time
                it[e] = DMLTestsData.E.ONE
                it[es] = DMLTestsData.E.ONE
                it[s] = sTest
                it[dc] = dec
            }

            tbl.checkRow(tbl.select { DMLTestsData.Misc.n.eq(42) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.nn.isNull() }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.nn.eq(null as Int?) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.d.eq(date) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.dn.isNull() }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.dn.eq(null as LocalDateTime?) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.t.eq(time) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.tn.isNull() }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.tn.eq(null as LocalDateTime?) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.e.eq(DMLTestsData.E.ONE) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.en.isNull() }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.en.eq(null as DMLTestsData.E?) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.s.eq(sTest) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.sn.isNull() }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.sn.eq(null as String?) }.single(), 42, null, date, null, time, null, DMLTestsData.E.ONE, null, DMLTestsData.E.ONE, null, sTest, null, dec, null, null, null)
        }
    }

    @Test
    fun testSelect02() {
        val tbl = DMLTestsData.Misc
        withTables(tbl) {
            val date = today
            val time = LocalDateTime.now()
            val sTest = "test"
            val eOne = DMLTestsData.E.ONE
            val dec = BIG_DECIMAL_VALUE
            tbl.insert {
                it[n] = 42
                it[nn] = 42
                it[d] = date
                it[dn] = date
                it[t] = time
                it[tn] = time
                it[e] = eOne
                it[en] = eOne
                it[es] = eOne
                it[esn] = eOne
                it[s] = sTest
                it[sn] = sTest
                it[dc] = dec
                it[dcn] = dec
                it[fcn] = 239.42f
                it[dblcn] = 567.89
            }

            tbl.checkRow(tbl.select { DMLTestsData.Misc.nn.eq(42) }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.nn.neq<Int?>(null) }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.dn.eq(date) }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.dn.isNotNull() }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.t.eq(time) }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.tn.isNotNull() }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.en.eq(eOne) }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.en.isNotNull() }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)

            tbl.checkRow(tbl.select { DMLTestsData.Misc.sn.eq(sTest) }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)
            tbl.checkRow(tbl.select { DMLTestsData.Misc.sn.isNotNull() }.single(), 42, 42, date, date, time, time, eOne, eOne, eOne, eOne, sTest, sTest, dec, dec, 239.42f, 567.89)
        }
    }

    @Test
    fun testUpdate02() {
        val tbl = DMLTestsData.Misc
        withTables(tbl) {
            val date = today
            val time = LocalDateTime.now()
            val eOne = DMLTestsData.E.ONE
            val sTest = "test"
            val dec = BIG_DECIMAL_VALUE
            tbl.insert {
                it[n] = 42
                it[nn] = 42
                it[d] = date
                it[dn] = date
                it[t] = time
                it[tn] = time
                it[e] = eOne
                it[en] = eOne
                it[es] = eOne
                it[esn] = eOne
                it[s] = sTest
                it[sn] = sTest
                it[dc] = dec
                it[dcn] = dec
                it[fcn] = 239.42f
            }

            tbl.update({ DMLTestsData.Misc.n.eq(42) }) {
                it[nn] = null
                it[dn] = null
                it[tn] = null
                it[en] = null
                it[esn] = null
                it[sn] = null
                it[dcn] = null
                it[fcn] = null
            }

            val row = tbl.selectAll().single()
            tbl.checkRow(row, 42, null, date, null, time, null, eOne, null, eOne, null, sTest, null, dec, null, null, null)
        }
    }

    @Test
    fun testUpdate03() {
        val tbl = DMLTestsData.Misc
        val date = today
        val time = LocalDateTime.now()
        val eOne = DMLTestsData.E.ONE
        val dec = BIG_DECIMAL_VALUE
        withTables(tables = *arrayOf(tbl)) {
            tbl.insert {
                it[n] = 101
                it[s] = "123456789"
                it[sn] = "123456789"
                it[d] = date
                it[t] = time
                it[e] = eOne
                it[es] = eOne
                it[dc] = dec
            }

            tbl.update({ DMLTestsData.Misc.n.eq(101) }) {
                it.update(s, s.substring(2, 255))
                it.update(sn, s.substring(3, 255))
            }

            val row = tbl.select { DMLTestsData.Misc.n eq 101 }.single()
            tbl.checkRow(row, 101, null, date, null, time, null, eOne, null, eOne, null, "23456789", "3456789", dec, null, null, null)
        }
    }

    @Test
    fun testJoinWithAlias01() {
        withCitiesAndUsers { _, users, _ ->
            val usersAlias = users.alias("u2")
            val resultRow = Join(users).join(usersAlias, JoinType.LEFT, usersAlias[DMLTestsData.Users.id], stringLiteral("smth"))
                .select { DMLTestsData.Users.id eq "alex" }.single()

            assert(resultRow[DMLTestsData.Users.name] == "Alex")
            assert(resultRow[usersAlias[DMLTestsData.Users.name]] == "Something")
        }
    }

    @Test
    fun testJoinWithJoin01() {
        withCitiesAndUsers { cities, users, userData ->
            val rows = (cities innerJoin (users innerJoin userData)).selectAll()
            assertEquals(2, rows.count())
        }
    }

    @Test
    fun testStringFunctions() {
        withCitiesAndUsers { cities, _, _ ->

            val lcase = DMLTestsData.Cities.name.lowerCase()
            assert(cities.slice(lcase).selectAll().any { it[lcase] == "prague" })

            val ucase = DMLTestsData.Cities.name.upperCase()
            assert(cities.slice(ucase).selectAll().any { it[ucase] == "PRAGUE" })
        }
    }

    @Test
    fun testJoinSubQuery01() {
        withCitiesAndUsers { _, users, _ ->
            val expAlias = DMLTestsData.Users.name.max().alias("m")
            val usersAlias = users.slice(DMLTestsData.Users.cityId, expAlias).selectAll().groupBy(DMLTestsData.Users.cityId).alias("u2")
            val resultRows = Join(users).join(usersAlias, JoinType.INNER, usersAlias[expAlias], DMLTestsData.Users.name).selectAll().toList()
            assertEquals(3, resultRows.size)
        }
    }

    @Test
    fun testJoinSubQuery02() {
        withCitiesAndUsers { _, users, _ ->
            val expAlias = DMLTestsData.Users.name.max().alias("m")

            val query = Join(users).joinQuery(on = { it[expAlias].eq(DMLTestsData.Users.name) }) {
                users.slice(DMLTestsData.Users.cityId, expAlias).selectAll().groupBy(DMLTestsData.Users.cityId)
            }
            val innerExp = query.lastQueryAlias!![expAlias]

            assertEquals("q0", query.lastQueryAlias?.alias)
            assertEquals(3, query.selectAll().count())
            assertNotNull(query.slice(users.columns + innerExp).selectAll().first()[innerExp])
        }
    }

    @Test
    fun testJoinWithAdditionalConstraint() {
        withCitiesAndUsers { cities, users, _ ->
            val usersAlias = users.alias("name")
            val join = cities.join(usersAlias, JoinType.INNER, DMLTestsData.Cities.id, usersAlias[DMLTestsData.Users.cityId]) {
                DMLTestsData.Cities.id greater 1 and (DMLTestsData.Cities.name.neq(usersAlias[DMLTestsData.Users.name]))
            }

            assertEquals(2, join.selectAll().count())
        }
    }

    @Test
    fun testDefaultExpressions01() {

        fun abs(value: Int) = object : ExpressionWithColumnType<Int>() {
            override fun toSQL(queryBuilder: QueryBuilder): String = "ABS($value)"

            override val columnType: IColumnType = IntegerColumnType()
        }

        val foo = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val name = text("name")
            val defaultDateTime = datetime("defaultDateTime").defaultExpression(CurrentDateTime())
            val defaultInt = integer("defaultInteger").defaultExpression(abs(-100))
        }

        withTables(foo) {
            val resultSet = foo.insert {
                it[foo.name] = "bar"
            }
            val result = foo.select {
                foo.id eq resultSet[foo.id]!!
            }.single()

            assertEquals(today.toLocalDate(), result[foo.defaultDateTime].toLocalDate())
            assertEquals(100, result[foo.defaultInt])
        }
    }

    @Test
    fun testDefaultExpressions02() {
        val foo = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val name = text("name")
            val defaultDateTime = datetime("defaultDateTime").defaultExpression(CurrentDateTime())
        }

        val nonDefaultDate = LocalDate.parse("2000-01-01").atStartOfDay()

        withTables(foo) {
            val resultSet = foo.insert {
                it[foo.name] = "bar"
                it[foo.defaultDateTime] = nonDefaultDate
            }

            val result = foo.select {
                foo.id eq resultSet[foo.id]!!
            }.single()

            assertEquals("bar", result[foo.name])
            assertEqualDateTime(nonDefaultDate, result[foo.defaultDateTime])

            foo.update({foo.id eq resultSet[foo.id]!!}) {
                it[foo.name] = "baz"
            }

            val result2 = foo.select { foo.id eq resultSet[foo.id]!! }.single()
            assertEquals("baz", result2[foo.name])
            assertEqualDateTime(nonDefaultDate, result2[foo.defaultDateTime])
        }
    }

    @Test
    fun testRandomFunction01() {
        val t = DMLTestsData.Cities
        withTables(t) {
            if (t.selectAll().count() == 0) {
                t.insert { it[name] = "city-1" }
            }

            val rand = Random()
            val resultRow = t.slice(rand).selectAll().limit(1).single()
            println(resultRow[rand])
        }
    }

    private fun assertQueryResultValid(query: Query) {
        query.forEach { row ->
            val userName = row[DMLTestsData.Users.name]
            val cityName = row[DMLTestsData.Cities.name]
            when (userName) {
                "Andrey" -> assertEquals(ST_PETERSBURG, cityName)
                "Sergey" -> assertEquals("Munich", cityName)
                else -> error("Unexpected user $userName")
            }
        }
    }

    private val predicate = Op.build {
        val nameCheck = (DMLTestsData.Users.id eq "andrey") or (DMLTestsData.Users.name eq "Sergey")
        val cityCheck = DMLTestsData.Users.cityId eq DMLTestsData.Cities.id
        nameCheck and cityCheck
    }

    @Test
    fun testAdjustQuerySlice() {
        withCitiesAndUsers { cities, users, _ ->
            val queryAdjusted = (users innerJoin cities)
                .slice(DMLTestsData.Users.name)
                .select(predicate)

            fun Query.sliceIt(): FieldSet = this.set.source.slice(DMLTestsData.Users.name, DMLTestsData.Cities.name)
            val oldSlice = queryAdjusted.set.fields
            val expectedSlice = queryAdjusted.sliceIt().fields
            queryAdjusted.adjustSlice { slice(DMLTestsData.Users.name, DMLTestsData.Cities.name) }
            val actualSlice = queryAdjusted.set.fields
            fun containsInAnyOrder(list: List<*>) = containsInAnyOrder(*list.toTypedArray())

            assertThat(oldSlice, not(containsInAnyOrder(actualSlice)))
            assertThat(actualSlice, containsInAnyOrder(expectedSlice))
            assertQueryResultValid(queryAdjusted)
        }
    }

    @Test
    fun testAdjustQueryColumnSet() {
        withCitiesAndUsers { cities, users, _ ->
            val queryAdjusted = users
                .slice(DMLTestsData.Users.name, DMLTestsData.Cities.name)
                .select(predicate)
            val oldColumnSet = queryAdjusted.set.source
            val expectedColumnSet = users innerJoin cities
            queryAdjusted.adjustColumnSet { innerJoin(cities) }
            val actualColumnSet = queryAdjusted.set.source
            fun ColumnSet.repr(): String = this.describe(TransactionManager.current(), QueryBuilder(false))

            assertNotEquals(oldColumnSet.repr(), actualColumnSet.repr())
            assertEquals(expectedColumnSet.repr(), actualColumnSet.repr())
            assertQueryResultValid(queryAdjusted)
        }
    }

    @Test
    fun testAdjustQueryWhere() {
        withCitiesAndUsers { cities, users, _ ->
            val queryAdjusted = (users innerJoin cities)
                .slice(DMLTestsData.Users.name, DMLTestsData.Cities.name)
                .selectAll()
            queryAdjusted.adjustWhere {
                assertNull(this)
                predicate
            }
            val actualWhere = queryAdjusted.where
            fun Op<Boolean>.repr(): String = this.toSQL(QueryBuilder(false))

            assertEquals(predicate.repr(), actualWhere!!.repr())
            assertQueryResultValid(queryAdjusted)
        }
    }

    @Test
    fun testQueryAndWhere() {
        withCitiesAndUsers { cities, users, _ ->
            val queryAdjusted = (users innerJoin cities)
                    .slice(DMLTestsData.Users.name, DMLTestsData.Cities.name)
                    .select{ predicate }

            queryAdjusted.andWhere {
                predicate
            }
            val actualWhere = queryAdjusted.where
            fun Op<Boolean>.repr(): String = this.toSQL(QueryBuilder(false))

            assertEquals((predicate.and(predicate)).repr(), actualWhere!!.repr())
            assertQueryResultValid(queryAdjusted)
        }
    }

    @Test
    fun `test that count() works with Query that contains distinct and columns with same name from different tables`() {
        withCitiesAndUsers { cities, users, _ ->
            assertEquals(3, cities.innerJoin(users).selectAll().withDistinct().count())
        }
    }

    @Test
    fun `test that count() works with Query that contains distinct and columns with same name from different tables and already defined alias`() {
        withCitiesAndUsers { cities, users, _ ->
            assertEquals(3, cities.innerJoin(users).slice(DMLTestsData.Users.id.alias("usersId"), DMLTestsData.Cities.id).selectAll().withDistinct().count())
        }
    }

    @Test
    fun `test that count() returns right value for Query with group by`() {
        withCitiesAndUsers { _, _, userData ->
            val uniqueUsersInData = userData.slice(DMLTestsData.UserData.user_id).selectAll().withDistinct().count()
            val sameQueryWithGrouping = userData.slice(DMLTestsData.UserData.value.max()).selectAll().groupBy(DMLTestsData.UserData.user_id).count()
            assertEquals(uniqueUsersInData, sameQueryWithGrouping)
        }

        withTables(OrgMemberships, Orgs) {
            val resultSet = Orgs.insert {
                it[name] = "FOo"
            }
            OrgMemberships.insert {
                it[orgId] = resultSet[Orgs.id]!!
            }

            assertEquals(1, OrgMemberships.selectAll().count())
        }
    }

    @Test
    fun testCompoundOp() {
        withCitiesAndUsers { _, users, _ ->
            val allUsers = setOf(
                    "Andrey",
                    "Sergey",
                    "Eugene",
                    "Alex",
                    "Something"
            )
            val orOp = allUsers.map { Op.build { DMLTestsData.Users.name eq it } }.compoundOr()
            val userNamesOr = users.select(orOp).map { it[DMLTestsData.Users.name] }.toSet()
            assertEquals(allUsers, userNamesOr)

            val andOp = allUsers.map { Op.build { DMLTestsData.Users.name eq it } }.compoundAnd()
            assertEquals(0, users.select(andOp).count())
        }
    }

    @Test
    fun testOrderByExpressions() {
        withCitiesAndUsers { cities, users, _ ->
            val expression = wrapAsExpression<Int>(users
                    .slice(DMLTestsData.Users.id.count())
                    .select {
                        DMLTestsData.Cities.id eq DMLTestsData.Users.cityId
                    })

            val result = cities
                    .selectAll()
                    .orderBy(expression, SortOrder.DESC)
                    .toList()

            // Munich - 2 users
            // St. Petersburg - 1 user
            // Prague - 0 users
            println(result)
        }
    }

    @Test fun testTRUEandFALSEOps() {
        withCitiesAndUsers { cities, _, _ ->
            val allSities = cities.selectAll().map { it[DMLTestsData.Cities.name] }
            assertEquals(0, cities.select { Op.FALSE }.count())
            assertEquals(allSities.size, cities.select { Op.TRUE }.count())
        }
    }
}

