[![Build Status](https://travis-ci.com/revolut-engineering/PgExposed.svg?branch=master)](https://travis-ci.com/revolut-engineering/PgExposed)
[![codecov](https://codecov.io/gh/Revolut-Engineering/PgExposed/branch/master/graph/badge.svg)](https://codecov.io/gh/Revolut-Engineering/PgExposed)
[![Download](https://api.bintray.com/packages/revolut/revolut/pgexposed/images/download.svg) ](https://bintray.com/revolut/revolut/pgexposed/_latestVersion)
[![GitHub License](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)

PgExposed - Kotlin PostgreSQL Library
==================

_PgExposed_ is a lightweight PostgreSQL library written over Postgres JDBC driver for [Kotlin](https://github.com/JetBrains/kotlin) language.
It does have two layers of database access: typesafe SQL wrapping DSL and lightweight data access objects

```gradle

maven {
    url  "https://dl.bintray.com/revolut/revolut"
    mavenContent {
        releasesOnly()
    }
}

compile 'com.revolut:pgexposed:1.0.x'
```

## Dialects

Currently supported database dialects:
* PostgreSQL

## SQL DSL sample:
```kotlin
import com.revolut.pgexposed.sql.*
import com.revolut.pgexposed.sql.transactions.transaction

object Users : Table() {
    val id = varchar("id", 10).primaryKey() // Column<String>
    val name = varchar("name", length = 50) // Column<String>
    val cityId = (integer("city_id") references Cities.id).nullable() // Column<Int?>
}

object Cities : Table() {
    val id = integer("id").autoIncrement().primaryKey() // Column<Int>
    val name = varchar("name", 50) // Column<String>
}

fun main(args: Array<String>) {
    Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")

    transaction {
        SchemaUtils.create (Cities, Users)

        val saintPetersburgId = Cities.insert {
            it[name] = "St. Petersburg"
        } get Cities.id

        val munichId = Cities.insert {
            it[name] = "Munich"
        } get Cities.id

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

        Users.update({Users.id eq "alex"}) {
            it[name] = "Alexey"
        }

        Users.deleteWhere{Users.name like "%thing"}

        println("All cities:")

        for (city in Cities.selectAll()) {
            println("${city[Cities.id]}: ${city[Cities.name]}")
        }

        println("Manual join:")
        (Users innerJoin Cities).slice(Users.name, Cities.name).
            select {(Users.id.eq("andrey") or Users.name.eq("Sergey")) and
                    Users.id.eq("sergey") and Users.cityId.eq(Cities.id)}.forEach {
            println("${it[Users.name]} lives in ${it[Cities.name]}")
        }

        println("Join with foreign key:")


        (Users innerJoin Cities).slice(Users.name, Users.cityId, Cities.name).
                select {Cities.name.eq("St. Petersburg") or Users.cityId.isNull()}.forEach {
            if (it[Users.cityId] != null) {
                println("${it[Users.name]} lives in ${it[Cities.name]}")
            }
            else {
                println("${it[Users.name]} lives nowhere")
            }
        }

        println("Functions and group by:")

        ((Cities innerJoin Users).slice(Cities.name, Users.id.count()).selectAll().groupBy(Cities.name)).forEach {
            val cityName = it[Cities.name]
            val userCount = it[Users.id.count()]

            if (userCount > 0) {
                println("$userCount user(s) live(s) in $cityName")
            } else {
                println("Nobody lives in $cityName")
            }
        }

        SchemaUtils.drop (Users, Cities)

    }
}

```

Outputs:
```
    SQL: CREATE TABLE IF NOT EXISTS Cities (id INT AUTO_INCREMENT NOT NULL, name VARCHAR(50) NOT NULL, CONSTRAINT pk_Cities PRIMARY KEY (id))
    SQL: CREATE TABLE IF NOT EXISTS Users (id VARCHAR(10) NOT NULL, name VARCHAR(50) NOT NULL, city_id INT NULL, CONSTRAINT pk_Users PRIMARY KEY (id))
    SQL: ALTER TABLE Users ADD FOREIGN KEY (city_id) REFERENCES Cities(id)
    SQL: INSERT INTO Cities (name) VALUES ('St. Petersburg')
    SQL: INSERT INTO Cities (name) VALUES ('Munich')
    SQL: INSERT INTO Cities (name) VALUES ('Prague')
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('andrey', 'Andrey', 1)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('sergey', 'Sergey', 2)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('eugene', 'Eugene', 2)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('alex', 'Alex', NULL)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('smth', 'Something', NULL)
    SQL: UPDATE Users SET name='Alexey' WHERE Users.id = 'alex'
    SQL: DELETE FROM Users WHERE Users.name LIKE '%thing'
    All cities:
    SQL: SELECT Cities.id, Cities.name FROM Cities
    1: St. Petersburg
    2: Munich
    3: Prague
    Manual join:
    SQL: SELECT Users.name, Cities.name FROM Users INNER JOIN Cities ON Cities.id = Users.city_id WHERE ((Users.id = 'andrey') or (Users.name = 'Sergey')) and Users.id = 'sergey' and Users.city_id = Cities.id
    Sergey lives in Munich
    Join with foreign key:
    SQL: SELECT Users.name, Users.city_id, Cities.name FROM Users INNER JOIN Cities ON Cities.id = Users.city_id WHERE (Cities.name = 'St. Petersburg') or (Users.city_id IS NULL)
    Andrey lives in St. Petersburg
    Functions and group by:
    SQL: SELECT Cities.name, COUNT(Users.id) FROM Cities INNER JOIN Users ON Cities.id = Users.city_id GROUP BY Cities.name
    1 user(s) live(s) in St. Petersburg
    2 user(s) live(s) in Munich
    SQL: DROP TABLE Users
    SQL: DROP TABLE Cities
```
