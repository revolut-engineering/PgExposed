package org.pgexposed.sql.tests

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import org.pgexposed.sql.*
import org.pgexposed.sql.tests.TestDB.POSTGRESQL
import org.pgexposed.sql.transactions.transaction
import org.pgexposed.sql.vendors.currentDialect
import java.util.*
import kotlin.concurrent.thread

enum class TestDB(val connection: () -> String, val driver: String, val user: String = "root", val pass: String = "",
                  val beforeConnection: () -> Unit = {}, val afterTestFinished: () -> Unit = {}, var db: Database? = null) {
    POSTGRESQL({"jdbc:postgresql://localhost:12346/template1?user=postgres&password=&lc_messages=en_US.UTF-8"}, "org.postgresql.Driver",
            beforeConnection = { postgresSQLProcess }, afterTestFinished = { postgresSQLProcess.close() });


    fun connect() = Database.connect(connection(), user = user, password = pass, driver = driver)
}

private val registeredOnShutdown = HashSet<TestDB>()

private val postgresSQLProcess by lazy {
    EmbeddedPostgres.builder()
        .setPgBinaryResolver{ system, _ ->
            EmbeddedPostgres::class.java.getResourceAsStream("/postgresql-$system-x86_64.txz")
        }/*.setLocaleConfig("locale", locale)*/
        .setPort(12346).start()
}

abstract class DatabaseTestsBase {
    fun withDb(statement: Transaction.() -> Unit) {

        val dbSettings = POSTGRESQL

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

        if (dbSettings !in registeredOnShutdown) {
            dbSettings.beforeConnection()
            Runtime.getRuntime().addShutdownHook(thread(false ){ dbSettings.afterTestFinished() })
            registeredOnShutdown += dbSettings
            dbSettings.db = dbSettings.connect()
        }

        val database = dbSettings.db!!

        val connection = database.connector()
        val transactionIsolation = connection.metaData.defaultTransactionIsolation
        connection.close()
        transaction(transactionIsolation, 1, db = database) {
            statement()
        }
    }

    fun withTables (vararg tables: Table, statement: Transaction.() -> Unit) {
        withDb {
            SchemaUtils.create(*tables)
            try {
                statement()
                commit() // Need commit to persist data before drop tables
            } finally {
                SchemaUtils.drop(*tables)
                commit()
            }
        }

    }

    fun <T> Transaction.assertEquals(exp: T, act: T) = kotlin.test.assertEquals(exp, act, "Failed on ${currentDialect.name}")
    fun <T> Transaction.assertEquals(exp: T, act: List<T>) = kotlin.test.assertEquals(exp, act.single(), "Failed on ${currentDialect.name}")
}
