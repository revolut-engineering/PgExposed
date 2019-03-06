package com.revolut.pgexposed.sql.tables

import com.revolut.pgexposed.sql.Table
import java.math.BigDecimal
import java.time.LocalDateTime

object DMLTestsData {
    object Cities : Table() {
        val id = integer("cityId").autoIncrement("cities_seq").primaryKey()
        val name = varchar("name", 50).uniqueIndex("city_name_idx")
    }

    object Users : Table() {
        val id = varchar("id", 10).primaryKey()
        val name = varchar("name", length = 50)
        val cityId = (integer("city_id") references DMLTestsData.Cities.id).nullable()
    }

    object UserData : Table() {
        val user_id = varchar("user_id", 10) references DMLTestsData.Users.id
        val comment = varchar("comment", 30)
        val value = integer("value")
    }

    enum class E {
        ONE,
        TWO,
        THREE
    }

    object Misc : Table() {
        val n = integer("n")
        val nn = integer("nn").nullable()

        val d = date("d")
        val dn = date("dn").nullable()

        val t = datetime("t")
        val tn = datetime("tn").nullable()

        val e = enumeration("e", DMLTestsData.E::class)
        val en = enumeration("en", DMLTestsData.E::class).nullable()

        val es = enumerationByName("es", 5, DMLTestsData.E::class)
        val esn = enumerationByName("esn", 5, DMLTestsData.E::class).nullable()

        val s = varchar("s", 100)
        val sn = varchar("sn", 100).nullable()

        val dc = decimal("dc", 12, 2)
        val dcn = decimal("dcn", 12, 2).nullable()

        val fcn = float("fcn").nullable()
        val dblcn = double("dblcn").nullable()

        val char = char("char").nullable()
    }

    const val ST_PETERSBURG = "St. Petersburg"

    val BIG_DECIMAL_VALUE = BigDecimal("239.42")
    val today: LocalDateTime = LocalDateTime.now().toLocalDate().atStartOfDay()
}