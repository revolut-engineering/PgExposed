package com.revolut.pgexposed.sql

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import com.revolut.pgexposed.exceptions.ExposedSQLException
import com.revolut.pgexposed.sql.transactions.TransactionManager
import com.revolut.pgexposed.sql.transactions.transaction
import org.junit.After
import org.junit.Test
import java.io.PrintWriter
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLTransientException
import java.util.logging.Logger
import javax.sql.DataSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

private open class DataSourceStub : DataSource {
    override fun setLogWriter(out: PrintWriter?): Unit = throw NotImplementedError()
    override fun getParentLogger(): Logger { throw NotImplementedError() }
    override fun setLoginTimeout(seconds: Int) { throw NotImplementedError() }
    override fun isWrapperFor(iface: Class<*>?): Boolean { throw NotImplementedError() }
    override fun getLogWriter(): PrintWriter { throw NotImplementedError() }
    override fun <T : Any?> unwrap(iface: Class<T>?): T { throw NotImplementedError() }
    override fun getConnection(): Connection { throw NotImplementedError() }
    override fun getConnection(username: String?, password: String?): Connection { throw NotImplementedError() }
    override fun getLoginTimeout(): Int { throw NotImplementedError() }
}

class ConnectionTimeoutTest : DatabaseTestsBase(){

    private class ExceptionOnGetConnectionDataSource : DataSourceStub() {
        var connectCount = 0

        override fun getConnection(): Connection {
            connectCount++
            throw GetConnectException()
        }
    }

    private class GetConnectException : SQLTransientException()

    @Test
    fun `connect fail causes repeated connect attempts`(){
        val datasource = ExceptionOnGetConnectionDataSource()
        val db = Database.connect(datasource = datasource)

        try {
            transaction(TransactionManager.manager.defaultIsolationLevel, 42, db) {
                exec("SELECT 1;")
                // NO OP
            }
            fail("Should have thrown ${GetConnectException::class.simpleName}")
        } catch (e : ExposedSQLException){
            assertTrue(e.cause is GetConnectException)
            kotlin.test.assertEquals(42, datasource.connectCount)
        }
    }
}

class ConnectionExceptions : DatabaseTestsBase() {

    abstract class ConnectionSpy(private val connection: Connection) : Connection by connection {
        var commitCalled = false
        var rollbackCalled = false
        var closeCalled = false

        override fun commit() {
            commitCalled = true
            throw CommitException()
        }

        override fun rollback() {
            rollbackCalled = true
        }

        override fun close() {
            closeCalled = true
        }
    }

    private class WrappingDataSource<T : Connection>(private val testDB: TestDB, private val connectionDecorator: (Connection) -> T) : DataSourceStub() {
        val connections = mutableListOf<T>()

        override fun getConnection(): Connection {
            val connection = DriverManager.getConnection(testDB.connection(), testDB.user, testDB.pass)
            val wrapped = connectionDecorator(connection)
            connections.add(wrapped)
            return wrapped
        }
    }

    private class RollbackException : SQLTransientException()
    private class ExceptionOnRollbackConnection(connection: Connection) : ConnectionSpy(connection) {
        override fun rollback() {
            super.rollback()
            throw RollbackException()
        }
    }

    @Test
    fun `transaction repetition works even if rollback throws exception`() {
        `_transaction repetition works even if rollback throws exception`(ConnectionExceptions::ExceptionOnRollbackConnection)
    }
    private fun `_transaction repetition works even if rollback throws exception`(connectionDecorator: (Connection) -> ConnectionSpy){
        val wrappingDataSource = WrappingDataSource(TestDB.POSTGRESQL, connectionDecorator)

        val db = Database.connect(datasource = wrappingDataSource)
        try {
            withDb {
                transaction(TransactionManager.manager.defaultIsolationLevel, 5, db) {
                    this.exec("BROKEN_SQL_THAT_CAUSES_EXCEPTION()")
                }
            }
            fail("Should have thrown an exception")
        } catch (e : SQLException){
            assertThat(e.toString(), Matchers.containsString("BROKEN_SQL_THAT_CAUSES_EXCEPTION"))
            assertEquals(5, wrappingDataSource.connections.size)
            wrappingDataSource.connections.forEach {
                assertFalse(it.commitCalled)
                assertTrue(it.rollbackCalled)
                assertTrue(it.closeCalled)
            }
        }
    }

    private class CommitException : SQLTransientException()
    private class ExceptionOnCommitConnection(connection: Connection) : ConnectionSpy(connection) {
        override fun commit() {
            super.commit()
            throw CommitException()
        }
    }

    @Test
    fun `transaction repetition works when commit throws exception`() {
        `_transaction repetition works when commit throws exception`(ConnectionExceptions::ExceptionOnCommitConnection)
    }
    private fun `_transaction repetition works when commit throws exception`(connectionDecorator: (Connection) -> ConnectionSpy) {
        val wrappingDataSource = WrappingDataSource(TestDB.POSTGRESQL, connectionDecorator)
        val db = Database.connect(datasource = wrappingDataSource)
        try {
            withDb {
                transaction(TransactionManager.manager.defaultIsolationLevel, 5, db) {
                    this.exec("SELECT 1;")
                }
            }
            fail("Should have thrown an exception")
        } catch (e: CommitException) {
            assertEquals(5, wrappingDataSource.connections.size)
            wrappingDataSource.connections.forEach {
                assertTrue(it.commitCalled)
                assertTrue(it.closeCalled)
            }
        }
    }

    @Test
    fun `transaction throws exception if all commits throws exception`(){
        `_transaction throws exception if all commits throws exception`(ConnectionExceptions::ExceptionOnCommitConnection)
    }
    private fun `_transaction throws exception if all commits throws exception`(connectionDecorator: (Connection) -> ConnectionSpy){
        val wrappingDataSource = WrappingDataSource(TestDB.POSTGRESQL, connectionDecorator)
        val db = Database.connect(datasource = wrappingDataSource)
        try {
            withDb {
                transaction(TransactionManager.manager.defaultIsolationLevel, 5, db) {
                    this.exec("SELECT 1;")
                }
            }
            fail("Should have thrown an exception")
        } catch (e : CommitException){
            // Yay
        }
    }

    private class CloseException : SQLTransientException()
    private class ExceptionOnRollbackCloseConnection(connection: Connection) : ConnectionSpy(connection) {
        override fun rollback() {
            super.rollback()
            throw RollbackException()
        }

        override fun close() {
            super.close()
            throw CloseException()
        }
    }

    @Test
    fun `transaction repetition works even if rollback and close throws exception`(){
        `_transaction repetition works even if rollback throws exception`(ConnectionExceptions::ExceptionOnRollbackCloseConnection)
    }

    @Test
    fun `transaction repetition works when commit and close throws exception`(){
        `_transaction repetition works when commit throws exception`(ConnectionExceptions::ExceptionOnCommitConnection)
    }

    private class ExceptionOnCommitCloseConnection(connection: Connection) : ConnectionSpy(connection) {
        override fun commit() {
            super.commit()
            throw CommitException()
        }

        override fun close() {
            super.close()
            throw CloseException()
        }
    }

    @Test
    fun `transaction throws exception if all commits and close throws exception`(){
        `_transaction throws exception if all commits throws exception`(ConnectionExceptions::ExceptionOnCommitCloseConnection)
    }

    @After
    fun `teardown`(){
        TransactionManager.resetCurrent(null)
    }
}
