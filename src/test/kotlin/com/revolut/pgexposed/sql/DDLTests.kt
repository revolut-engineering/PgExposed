package com.revolut.pgexposed.sql

import com.revolut.pgexposed.sql.transactions.TransactionManager
import com.revolut.pgexposed.sql.postgres.PostgreSQLDialect
import com.revolut.pgexposed.sql.postgres.PostgresFunctionProvider
import com.revolut.pgexposed.sql.postgres.currentDialect
import com.revolut.pgexposed.sql.tables.DMLTestsData
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.sql.rowset.serial.SerialBlob

@Suppress("unused", "LocalVariableName")
class DDLTests : DatabaseTestsBase() {
    @Test fun tableExists01() {
        val TestTable = object : Table() {
            val id = integer("id").primaryKey()
            val name = varchar("name", length = 42)
        }

        withTables {
            assertEquals(false, TestTable.exists())
        }
    }

    @Test fun tableExists02() {
        val TestTable = object : Table("TableExists") {
            val id = integer("id").primaryKey()
            val name = varchar("name", length = 42)
        }

        withTables(TestTable) {
            assertEquals(true, TestTable.exists())
        }
    }

    object KeyWordTable : Table(name ="keywords") {
        val id = integer("id").autoIncrement().primaryKey()
        val bool = bool("bool")
    }

    @Test fun tableExistsWithKeyword() {
        withTables(KeyWordTable) {
            assertEquals(true, KeyWordTable.exists())
            KeyWordTable.insert {
                it[bool] = true
            }
        }
    }


    @Test fun testCreateMissingTablesAndColumns01() {
        val TestTable = object : Table("test_table") {
            val id = integer("id").primaryKey()
            val name = varchar("name", length = 42)
            val time = datetime("time").uniqueIndex()
        }

        withTables(tables = *arrayOf(TestTable)) {
            SchemaUtils.createMissingTablesAndColumns(TestTable)
            assertTrue(TestTable.exists())
            SchemaUtils.drop(TestTable)
        }
    }

    @Test fun testCreateMissingTablesAndColumns02() {
        val TestTable = object : Table("Users2") {
            val id: Column<String> = varchar("id", 64).clientDefault { UUID.randomUUID().toString() }.primaryKey()

            val name = varchar("name", 255)
            val email = varchar("email", 255).uniqueIndex()
        }

        withDb {
            SchemaUtils.createMissingTablesAndColumns(TestTable)
            assertTrue(TestTable.exists())
            try {
                SchemaUtils.createMissingTablesAndColumns(TestTable)
            } finally {
                SchemaUtils.drop(TestTable)
            }
        }
    }

    @Test fun testCreateMissingTablesAndColumnsChangeNullability() {
        val t1 = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = varchar("foo", 50)
        }

        val t2 = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = varchar("foo", 50).nullable()
        }

        withDb {
            SchemaUtils.createMissingTablesAndColumns(t1)
            t1.insert { it[foo] = "ABC" }
            assertFailAndRollback("Can't insert to not-null column") {
                t2.insert { it[foo] = null }
            }

            SchemaUtils.createMissingTablesAndColumns(t2)
            t2.insert { it[foo] = null }
            assertFailAndRollback("Can't make column non-null while has null value") {
                SchemaUtils.createMissingTablesAndColumns(t1)
            }

            t2.deleteWhere { t2.foo.isNull() }

            SchemaUtils.createMissingTablesAndColumns(t1)
            assertFailAndRollback("Can't insert to nullable column") {
                t2.insert { it[foo] = null }
            }
            SchemaUtils.drop(t1)
        }
    }

    @Test fun testCreateMissingTablesAndColumnsChangeCascadeType() {
        val fooTable = object : Table("foo") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = varchar("foo", 50)
        }

        val barTable1 = object : Table("bar") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = (integer("foo").references(fooTable.id, onDelete = ReferenceOption.NO_ACTION)).nullable()
        }

        val barTable2 = object : Table("bar") {
            val id = integer("id").autoIncrement().primaryKey()
            val foo = (integer("foo").references(fooTable.id, onDelete = ReferenceOption.CASCADE)).nullable()
        }

        withTables(fooTable, barTable1) {
            SchemaUtils.createMissingTablesAndColumns(barTable2)
        }
    }

    // Placed outside test function to shorten generated name
    object UnNamedTable : Table() {
        val id = integer("id").primaryKey()
        val name = varchar("name", length = 42)
    }

    @Test fun unnamedTableWithQuotesSQL() {
        withTables(UnNamedTable) {
            assertEquals("CREATE TABLE IF NOT EXISTS unnamed (${"id".inProperCase()} " +
                    "INT PRIMARY KEY, ${"name".inProperCase()} VARCHAR(42) NOT NULL)", UnNamedTable.ddl)
        }
    }

    @Test fun namedEmptyTableWithoutQuotesSQL() {
        val TestTable = object : Table("test_named_table") {
        }

        withDb {
            assertEquals("CREATE TABLE IF NOT EXISTS ${"test_named_table".inProperCase()}", TestTable.ddl)
            DMLTestsData.Users.select {
                exists(DMLTestsData.UserData.select { DMLTestsData.Users.id eq DMLTestsData.UserData.user_id })
            }
        }
    }

    @Test fun tableWithDifferentColumnTypesSQL01() {
        val TestTable = object : Table("different_column_types") {
            val id = integer("id").autoIncrement()
            val name = varchar("name", 42).primaryKey()
            val age = integer("age").nullable()
            // not applicable in H2 database
            //            val testCollate = varchar("testCollate", 2, "ascii_general_ci")
        }

        withTables(tables = *arrayOf(TestTable)) {
            val shortAutoIncType = "SERIAL"

            assertEquals("CREATE TABLE IF NOT EXISTS " + "${"different_column_types".inProperCase()} " +
                    "(${"id".inProperCase()} $shortAutoIncType NOT NULL, ${"name".inProperCase()} VARCHAR(42) PRIMARY KEY, " +
                    "${"age".inProperCase()} INT NULL)", TestTable.ddl)
        }
    }

    @Test fun tableWithDifferentColumnTypesSQL02() {
        val TestTable = object : Table("with_different_column_types") {
            val id = integer("id").primaryKey()
            val name = varchar("name", 42).primaryKey()
            val age = integer("age").nullable()
        }

        withTables(tables = *arrayOf(TestTable)) {
            assertEquals("CREATE TABLE IF NOT EXISTS " + "${"with_different_column_types".inProperCase()} " +
                    "(${"id".inProperCase()} INT, ${"name".inProperCase()} VARCHAR(42), ${"age".inProperCase()} INT NULL, " +
                    "CONSTRAINT pk_with_different_column_types PRIMARY KEY (${"id".inProperCase()}, ${"name".inProperCase()}))", TestTable.ddl)
        }
    }

    @Test fun testDefaults01() {
        val currentDT = CurrentDateTime()
        val nowExpression = object : Expression<LocalDateTime>() {
            override fun toSQL(queryBuilder: QueryBuilder) = "NOW()"
        }
        val dtConstValue = LocalDate.parse("2010-01-01").atStartOfDay()
        val dtLiteral = dateLiteral(dtConstValue)
        val TestTable = object : Table("t") {
            val id = integer("id").autoIncrement().primaryKey()
            val s = varchar("s", 100).default("test")
            val sn = varchar("sn", 100).default("testNullable").nullable()
            val l = long("l").default(42)
            val c = char("c").default('X')
            val t1 = datetime("t1").defaultExpression(currentDT)
            val t2 = datetime("t2").defaultExpression(nowExpression)
            val t3 = datetime("t3").defaultExpression(dtLiteral)
            val t4 = date("t4").default(dtConstValue)
        }

        fun Expression<*>.itOrNull() = "DEFAULT ${PostgresFunctionProvider.processForDefaultValue(this)} NOT NULL"

        withTables(TestTable) {
            val dtType = "TIMESTAMP"
            assertEquals("CREATE TABLE " + "IF NOT EXISTS " +
                    "${"t".inProperCase()} (" +
                    "${"id".inProperCase()} SERIAL PRIMARY KEY, " +
                    "${"s".inProperCase()} VARCHAR(100) DEFAULT 'test' NOT NULL, " +
                    "${"sn".inProperCase()} VARCHAR(100) DEFAULT 'testNullable' NULL, " +
                    "${"l".inProperCase()} BIGINT DEFAULT 42 NOT NULL, " +
                    "${"c".inProperCase()} CHAR DEFAULT 'X' NOT NULL, " +
                    "${"t1".inProperCase()} $dtType ${currentDT.itOrNull()}, " +
                    "${"t2".inProperCase()} $dtType ${nowExpression.itOrNull()}, " +
                    "${"t3".inProperCase()} $dtType ${dtLiteral.itOrNull()}, " +
                    "${"t4".inProperCase()} DATE ${dtLiteral.itOrNull()}" +
                    ")", TestTable.ddl)

            val resultRow = TestTable.insert {  }

            val row1 = TestTable.select {
                TestTable.id eq resultRow[TestTable.id]!!
            }.single()

            assertEquals("test", row1[TestTable.s])
            assertEquals("testNullable", row1[TestTable.sn])
            assertEquals(42, row1[TestTable.l])
            assertEquals('X', row1[TestTable.c])
            assertEqualDateTime(dtConstValue, row1[TestTable.t3])
            assertEqualDateTime(dtConstValue, row1[TestTable.t4])

            val resultSet = TestTable.insert {
                it[TestTable.sn] = null
            }

            TestTable.select { TestTable.id eq resultSet[TestTable.id]!! }.single()
        }
    }

    @Test fun testIndices01() {
        val t = object : Table("t1") {
            val id = integer("id").primaryKey()
            val name = varchar("name", 255).index()
        }

        withTables(t) {
            val alter = SchemaUtils.createIndex(t.indices[0])
            assertEquals("CREATE INDEX ${"t1_name".inProperCase()} ON ${"t1".inProperCase()} (${"name".inProperCase()})", alter)
        }
    }

    @Test fun testIndices02() {
        val t = object : Table("t2") {
            val id = integer("id").primaryKey()
            val lvalue = integer("lvalue")
            val rvalue = integer("rvalue")
            val name = varchar("name", 255).index()

            init {
                index (false, lvalue, rvalue)
            }
        }

        withTables(t) {
            val a1 = SchemaUtils.createIndex(t.indices[0])
            assertEquals("CREATE INDEX ${"t2_name".inProperCase()} ON ${"t2".inProperCase()} (${"name".inProperCase()})", a1)

            val a2 = SchemaUtils.createIndex(t.indices[1])
            assertEquals("CREATE INDEX ${"t2_lvalue_rvalue".inProperCase()} ON ${"t2".inProperCase()} " +
                    "(${"lvalue".inProperCase()}, ${"rvalue".inProperCase()})", a2)
        }
    }

    @Test fun testUniqueIndices01() {
        val t = object : Table("t1") {
            val id = integer("id").primaryKey()
            val name = varchar("name", 255).uniqueIndex()
        }

        withTables(t) {
            val alter = SchemaUtils.createIndex(t.indices[0])
            assertEquals("ALTER TABLE ${"t1".inProperCase()} ADD CONSTRAINT ${"t1_name_unique".inProperCase()} UNIQUE (${"name".inProperCase()})", alter)
        }
    }

    @Test fun testUniqueIndicesCustomName() {
        val t = object : Table("t1") {
            val id = integer("id").primaryKey()
            val name = varchar("name", 255).uniqueIndex("U_T1_NAME")
        }

        withTables(t) {
            val alter = SchemaUtils.createIndex(t.indices[0])
            assertEquals("ALTER TABLE ${"t1".inProperCase()} ADD CONSTRAINT ${"U_T1_NAME"} UNIQUE (${"name".inProperCase()})", alter)
        }
    }

    @Test fun testCompositePrimaryKeyCreateTable() {
        val tableName = "Foo"
        val t = object : Table(tableName) {
            val id1 = integer("id1").primaryKey()
            val id2 = integer("id2").primaryKey()
        }

        withTables(t) {
            val id1ProperName = t.id1.name.inProperCase()
            val id2ProperName = t.id2.name.inProperCase()

            assertEquals(
                    "CREATE TABLE IF NOT EXISTS " + "${tableName.inProperCase()} (" +
                            "${t.columns.joinToString { it.descriptionDdl() }}, " +
                            "CONSTRAINT pk_$tableName PRIMARY KEY ($id1ProperName, $id2ProperName)" +
                            ")",
                    t.ddl)
        }
    }

    @Test fun testAddCompositePrimaryKeyToTable() {
        val tableName = "Foo"
        val t = object : Table(tableName) {
            val id1 = integer("id1").primaryKey()
            val id2 = integer("id2").primaryKey()
        }

        withTables(tables = *arrayOf(t)) {
            val tableProperName = tableName.inProperCase()
            val id1ProperName = t.id1.name.inProperCase()
            val ddlId1 = t.id1.ddl
            val id2ProperName = t.id2.name.inProperCase()
            val ddlId2 = t.id2.ddl

            assertEquals(1, ddlId1.size)
            assertEquals("ALTER TABLE $tableProperName ADD ${t.id1.descriptionDdl()}", ddlId1.first())

            assertEquals(1, ddlId2.size)
            assertEquals("ALTER TABLE $tableProperName ADD ${t.id2.descriptionDdl()}, ADD CONSTRAINT pk_$tableName PRIMARY KEY ($id1ProperName, $id2ProperName)", ddlId2.first())
        }
    }

    @Test fun testMultiColumnIndex() {
        val t = object : Table("t1") {
            val type = varchar("type", 255)
            val name = varchar("name", 255)
            init {
                index(false, name, type)
                uniqueIndex(type, name)
            }
        }

        withTables(t) {
            val indexAlter = SchemaUtils.createIndex(t.indices[0])
            val uniqueAlter = SchemaUtils.createIndex(t.indices[1])
            assertEquals("CREATE INDEX ${"t1_name_type".inProperCase()} ON ${"t1".inProperCase()} (${"name".inProperCase()}, ${"type".inProperCase()})", indexAlter)
            assertEquals("ALTER TABLE ${"t1".inProperCase()} ADD CONSTRAINT ${"t1_type_name_unique".inProperCase()} UNIQUE (${"type".inProperCase()}, ${"name".inProperCase()})", uniqueAlter)
        }
    }

    @Test fun testMultiColumnIndexCustomName() {
        val t = object : Table("t1") {
            val type = varchar("type", 255)
            val name = varchar("name", 255)
            init {
                index("I_T1_NAME_TYPE", false, name, type)
                uniqueIndex("U_T1_TYPE_NAME", type, name)
            }
        }

        withTables(t) {
            val indexAlter = SchemaUtils.createIndex(t.indices[0])
            val uniqueAlter = SchemaUtils.createIndex(t.indices[1])
            assertEquals("CREATE INDEX ${"I_T1_NAME_TYPE"} ON ${"t1".inProperCase()} (${"name".inProperCase()}, ${"type".inProperCase()})", indexAlter)
            assertEquals("ALTER TABLE ${"t1".inProperCase()} ADD CONSTRAINT ${"U_T1_TYPE_NAME"} UNIQUE (${"type".inProperCase()}, ${"name".inProperCase()})", uniqueAlter)
        }
    }

    @Test fun testBlob() {
        val t = object: Table("t1") {
            val id = integer("id").autoIncrement("t1_seq").primaryKey()
            val b = blob("blob")
        }

        withTables(t) {
            val bytes = "Hello there!".toByteArray()
            val blob = SerialBlob(bytes)

            val id = t.insert {
                it[t.b] = blob
            } get (t.id)


            val readOn = t.select{t.id eq id!!}.first()[t.b]
            val text = readOn.binaryStream.reader().readText()

            assertEquals("Hello there!", text)
        }
    }

    @Test fun testBinary() {
        val t = object : Table("BinaryTable") {
            val binary = binary("bytes", 10)
        }

        withTables(t) {
            t.insert { it[t.binary] = "Hello!".toByteArray() }

            val bytes = t.selectAll().single()[t.binary]

            assertEquals("Hello!", String(bytes))
        }
    }

    @Test fun addAutoPrimaryKey() {
        val tableName = "Foo"
        val initialTable = object : Table(tableName) {
            val bar = text("bar")
        }
        val t = object: Table(tableName) {
            val id = integer("id").autoIncrement().primaryKey()
        }

        withDb {
            SchemaUtils.createMissingTablesAndColumns(initialTable)
            assertEquals("ALTER TABLE ${tableName.inProperCase()} ADD ${"id".inProperCase()} ${t.id.columnType.sqlType()} PRIMARY KEY", t.id.ddl.first())
            assertEquals(1, currentDialect.tableColumns(t).getValue(t).size)
            SchemaUtils.createMissingTablesAndColumns(t)
            assertEquals(2, currentDialect.tableColumns(t).getValue(t).size)
            SchemaUtils.drop(t)
        }

        withTables(tables = *arrayOf(initialTable)) {
            assertEquals("ALTER TABLE ${tableName.inProperCase()} ADD ${"id".inProperCase()} ${t.id.columnType.sqlType()} PRIMARY KEY", t.id.ddl)
            assertEquals(1, currentDialect.tableColumns(t).getValue(t).size)
            SchemaUtils.createMissingTablesAndColumns(t)
            assertEquals(2, currentDialect.tableColumns(t).getValue(t).size)
        }
    }

    @Test fun complexTest01() {
        val User = object : Table("User") {
            val id = integer("id").autoIncrement().primaryKey()
            val name = varchar("name", 255)
            val email = varchar("email", 255)
        }

        val Repository = object : Table("Repository") {
            val id = integer("id").autoIncrement().primaryKey()
            val name = varchar("name", 255)
        }

        val UserToRepo = object : Table("UserToRepo") {
            val id = integer("id").autoIncrement().primaryKey()
            val user = (integer("user") references User.id)
            val repo = (integer("repo") references Repository.id)
        }

        withTables(User, Repository, UserToRepo) {
            User.insert {
                it[User.name] = "foo"
                it[User.email] = "bar"
            }

            val userID = User.selectAll().single()[User.id]

            Repository.insert {
                it[Repository.name] = "foo"
            }
            val repo = Repository.selectAll().single()[Repository.id]

            UserToRepo.insert {
                it[UserToRepo.user] = userID
                it[UserToRepo.repo] = repo
            }

            assertEquals(1, UserToRepo.selectAll().count())
            UserToRepo.insert {
                it[UserToRepo.user] = userID
                it[UserToRepo.repo] = repo
            }

            assertEquals(2, UserToRepo.selectAll().count())
        }
    }

    object Table1 : Table() {
        val id = integer("id").autoIncrement().primaryKey()
        val table2 = integer("table2").references(Table2.id, onDelete = ReferenceOption.NO_ACTION)
    }

    object Table2 : Table() {
        val id = integer("id").autoIncrement().primaryKey()
        val table1 = integer("table1").references(Table1.id, onDelete = ReferenceOption.NO_ACTION).nullable()
    }

    @Test fun testCrossReference() {
        withTables(Table1, Table2) {
            val resultSet = Table2.insert{}
            val resultSet2 = Table1.insert {
                it[table2] = resultSet[Table2.id]!!
            }

            Table2.insert {
                it[table1] = resultSet2[Table1.id]!!
            }

            assertEquals(1, Table1.selectAll().count())
            assertEquals(2, Table2.selectAll().count())

            Table2.update {
                it[table1] = null
            }

            Table1.deleteAll()
            Table2.deleteAll()

            exec(ForeignKeyConstraint.from(Table2.table1).dropStatement().single())
        }
    }

    @Test fun testBooleanColumnType() {
        val BoolTable = object: Table("booleanTable") {
            val bool = bool("bool")
        }

        withTables(BoolTable){
            BoolTable.insert {
                it[bool] = true
            }
            val result = BoolTable.selectAll().toList()
            assertEquals(1, result.size)
            assertEquals(true, result.single()[BoolTable.bool])
        }
    }

    @Test fun testDeleteMissingTable() {
        val missingTable = Table("missingTable")
        withDb {
            SchemaUtils.drop(missingTable)
        }
    }

    @Test fun testCheckConstraint01() {
        val checkTable = object : Table("checkTable") {
            val positive = integer("positive").check { it greaterEq 0 }
            val negative = integer("negative").check("subZero") { it less 0 }
        }

        withTables(checkTable) {
            checkTable.insert {
                it[positive] = 42
                it[negative] = -14
            }

            assertEquals(1, checkTable.selectAll().count())

            assertFailAndRollback("Check constraint 1") {
                checkTable.insert {
                    it[positive] = -472
                    it[negative] = -354
                }
            }

            assertFailAndRollback("Check constraint 2") {
                checkTable.insert {
                    it[positive] = 538
                    it[negative] = 915
                }
            }
        }
    }

    @Test fun testCheckConstraint02() {
        val checkTable = object : Table("multiCheckTable") {
            val positive = integer("positive")
            val negative = integer("negative")

            init {
                check("multi") { (negative less 0) and (positive greaterEq 0) }
            }
        }

        withTables(checkTable) {
            checkTable.insert {
                it[positive] = 57
                it[negative] = -32
            }

            assertEquals(1, checkTable.selectAll().count())

            assertFailAndRollback("Check constraint 1") {
                checkTable.insert {
                    it[positive] = -47
                    it[negative] = -35
                }
            }

            assertFailAndRollback("Check constraint 2") {
                checkTable.insert {
                    it[positive] = 53
                    it[negative] = 91
                }
            }
        }
    }
}

private fun String.inProperCase(): String = TransactionManager.currentOrNull()?.let {
    (currentDialect as? PostgreSQLDialect)?.run {
        this@inProperCase.inProperCase
    }
} ?: this
