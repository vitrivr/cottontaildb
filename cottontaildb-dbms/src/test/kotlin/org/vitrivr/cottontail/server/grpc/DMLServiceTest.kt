package org.vitrivr.cottontail.server.grpc

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.CreateIndex
import org.vitrivr.cottontail.client.language.ddl.OptimizeEntity
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.TestConstants.DOUBLE_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.INT_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.STRING_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.TEST_COLLECTION_SIZE
import org.vitrivr.cottontail.test.TestConstants.TEST_ENTITY_NAME
import org.vitrivr.cottontail.test.TestConstants.TEST_SCHEMA
import org.vitrivr.cottontail.test.TestConstants.TEST_VECTOR_ENTITY_NAME
import org.vitrivr.cottontail.test.TestConstants.TWOD_COLUMN_NAME
import org.vitrivr.cottontail.utilities.math.random.nextInt
import kotlin.time.ExperimentalTime

/**
 * Integration tests that test the DML endpoint of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DMLServiceTest : AbstractClientTest() {

    @BeforeEach
    fun beforeEach() {
        this.startAndPopulateCottontail()
    }

    @AfterAll
    fun afterAll() {
        this.cleanup()
    }

    @AfterEach
    fun afterEach() {
        GrpcTestUtils.dropTestSchema(this.client)
    }

    /**
     * Performs a large number of INSERTs in a single transaction and checks the count before and afterwards.
     */
    @Test
    fun testSingleTxBatchInsert() {
        val entries = TEST_COLLECTION_SIZE * 5
        val batchSize = 1000
        val stringLength = 200

        /* Start large insert. */
        val txId = this.client.begin()
        repeat(entries / batchSize) { i ->
            val batch = BatchInsert().into(TEST_ENTITY_NAME.fqn).columns(INT_COLUMN_NAME, STRING_COLUMN_NAME, DOUBLE_COLUMN_NAME)
            repeat(batchSize) {
                j -> batch.append(i*j, RandomStringUtils.randomAlphanumeric(stringLength), 1.0)
            }
            this.client.insert(batch.txId(txId))
        }
        Assertions.assertEquals(TEST_COLLECTION_SIZE.toLong(), countElements(this.client, TEST_ENTITY_NAME)) /* Check before commit. */
        this.client.commit(txId)
        Assertions.assertEquals((entries + TEST_COLLECTION_SIZE).toLong(), countElements(this.client, TEST_ENTITY_NAME))  /* Check after commit. */
    }

    /**
     * Does multiple inserts on a lucene entity in different configurations.
     * This test runs for pretty long and is used to find checksum bugs which arise when files are split. (?)
     */
    @Test
    fun testSingleTxBatchInsertWithLuceneIndex() {
        val entries = TEST_COLLECTION_SIZE * 5
        val batchSize = 10000
        val stringLength = 200

        /* Create Lucene Index. */
        GrpcTestUtils.createLuceneIndexOnTestEntity(this.client)

        /* Start large insert. */
        val txId = this.client.begin()
        repeat(entries / batchSize) { i ->
            val batch = BatchInsert().into(TEST_ENTITY_NAME.fqn).columns(INT_COLUMN_NAME, STRING_COLUMN_NAME, DOUBLE_COLUMN_NAME).txId(txId)
            repeat(batchSize) { j -> batch.append(i*j, RandomStringUtils.randomAlphanumeric(stringLength), 1.0) }
            this.client.insert(batch)
        }

        Assertions.assertEquals(TEST_COLLECTION_SIZE.toLong(), countElements(this.client, TEST_ENTITY_NAME)) /* Check before commit. */
        this.client.commit(txId)
        Assertions.assertEquals((entries + TEST_COLLECTION_SIZE).toLong(), countElements(this.client, TEST_ENTITY_NAME))  /* Check after commit. */
    }

    /**
     * Tries to execute an INSERT and runs a 'concurrent' query in a different transaction. First query should not see INSERT, second query should.
     */
    @Test
    fun testMultiTxInsertWithCommitAndQuery() {
        /* Execute transaction 1. */
        val random = JDKRandomGenerator()
        val tx1 = this.client.begin()
        val string1 = RandomStringUtils.randomAlphabetic(6)
        val insert1 = Insert(TEST_VECTOR_ENTITY_NAME.fqn).values(
            STRING_COLUMN_NAME to string1,
            INT_COLUMN_NAME to random.nextInt(0, 10),
            TWOD_COLUMN_NAME to floatArrayOf(0.0f, 0.0f)
        ).txId(tx1)
        this.client.insert(insert1)

        /* Check results; insert 1 should exist. */
        val result1 = this.client.query(Query(TEST_VECTOR_ENTITY_NAME.fqn).where(Expression(STRING_COLUMN_NAME, "=", string1)).count())
        Assertions.assertEquals(0L, result1.next().asLong(0))

        /* Execute commits and check. */
        Assertions.assertDoesNotThrow { this.client.commit(tx1) }

        /* Check results; insert 2 should not exist. */
        val result2 = this.client.query(Query(TEST_VECTOR_ENTITY_NAME.fqn).where(Expression(STRING_COLUMN_NAME, "=", string1)).count())
        Assertions.assertEquals(1L, result2.next().asLong(0))
    }

    /**
     * Executes an UPDATE and checks if all values were changed.
     */
    @Test
    fun testSingleTxUpdateColumns() {
        val newValue = RandomStringUtils.randomAlphabetic(4)

        /* Perform update and sanity checks. */
        val update = Update().from(TEST_ENTITY_NAME.fqn).values(Pair(STRING_COLUMN_NAME, newValue))
        val r1 = this.client.update(update)
        Assertions.assertTrue(r1.hasNext())
        val el1 = r1.next()
        Assertions.assertEquals(TEST_COLLECTION_SIZE.toLong(), el1.asLong(0))

        /* Query and check values. */
        val select = Query().select("*").from(TEST_ENTITY_NAME.fqn)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertEquals(newValue, el2.asString(STRING_COLUMN_NAME))
        }
    }

    /**
     * Executes a series of UPDATES in a single transaction and checks, if those UPDATES were materialized.
     */
    @Test
    fun testSingleTxUpdateWithCommitAndQuery() {
        /* Query and update values. */
        val txId = this.client.begin()
        val s1 = Query().from(TEST_ENTITY_NAME.fqn).order(STRING_COLUMN_NAME, Direction.ASC).limit(100).txId(txId)
        val r1 = this.client.query(s1)
        for (el1 in r1) {
            /* Determine how many rows will be affected. */
            val count = Query().from(TEST_ENTITY_NAME.fqn)
                .where(Expression(STRING_COLUMN_NAME, "=", el1.asString(STRING_COLUMN_NAME)!!))
                .count()
            val c = this.client.query(count).next().asLong(0)

            /* Execute the update. */
            val update = Update().from(TEST_ENTITY_NAME.fqn)
                .values(Pair(INT_COLUMN_NAME, -1))
                .where(Expression(STRING_COLUMN_NAME, "=", el1.asString(STRING_COLUMN_NAME)!!))
                .txId(txId)
            val r2 = this.client.update(update)
            Assertions.assertTrue(r2.hasNext())
            val el2 = r2.next()
            Assertions.assertEquals(c, el2.asLong(0))
        }
        this.client.commit(txId)

        /* Query and check values. */
        val select = Query().select("*").from(TEST_ENTITY_NAME.fqn).order(STRING_COLUMN_NAME, Direction.ASC).limit(100)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertEquals(-1, el2.asInt(INT_COLUMN_NAME))
        }
    }

    /**
     * Executes a series of UPDATES in a single transaction followed by a ROLLBACK and checks, that those UPDATES were not materialized.
     */
    @Test
    fun testSingleTxUpdateWithRollbackAndQuery() {
        /* Query and update values. */
        val txId = this.client.begin()
        val s1 = Query().select("*").from(TEST_ENTITY_NAME.fqn).order(STRING_COLUMN_NAME, Direction.ASC).limit(100).txId(txId)
        val r1 = this.client.query(s1)
        for (el1 in r1) {
            /* Determine how many rows will be affected. */
            val count = Query().from(TEST_ENTITY_NAME.fqn)
                .where(Expression(STRING_COLUMN_NAME, "=", el1.asString(STRING_COLUMN_NAME)!!))
                .count()
            val c = this.client.query(count).next().asLong(0)

            /* Execute the update. */
            val update = Update().from(TEST_ENTITY_NAME.fqn)
                .values(Pair(INT_COLUMN_NAME, -1))
                .where(Expression(STRING_COLUMN_NAME, "=", el1.asString(STRING_COLUMN_NAME)!!))
                .txId(txId)
            val r2 = this.client.update(update)
            Assertions.assertTrue(r2.hasNext())
            val el2 = r2.next()
            Assertions.assertEquals(c, el2.asLong(0))
        }
        this.client.rollback(txId)

        /* Query and check values. */
        val select = Query().select("*").from(TEST_ENTITY_NAME.fqn).order(STRING_COLUMN_NAME, Direction.ASC).limit(100)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertNotEquals(-1, el2.asInt(INT_COLUMN_NAME))
        }
    }

    /**
     * Does multiple inserts on a lucene entity in different configurations.
     * This test runs for pretty long and is used to find checksum bugs which arise when files are split. (?)
     */
    @Test
    fun testMultiInsertLucene() {
        val entityName = TEST_SCHEMA.entity("lucenetest")
        val batchCount = 10000
        val repeatBatchInsert = 100
        val stringLength = 200
        var txId = client.begin()
        this.client.create( CreateEntity(entityName.fqn).column(STRING_COLUMN_NAME, Type.STRING).txId(txId))
        this.client.create(CreateIndex(entityName.fqn, STRING_COLUMN_NAME, CottontailGrpc.IndexType.LUCENE).txId(txId))
        this.client.commit(txId)

        // we have an outer loop to check if optimization is the problem.
        // The first inserts are all made without optimization in between, and between the first and the second is an optimize
        repeat(10) {
            repeat(repeatBatchInsert / 10) {
                txId = client.begin()
                val batch = BatchInsert().into(entityName.fqn).columns(STRING_COLUMN_NAME).txId(txId)
                repeat(batchCount) {
                    batch.append(
                        RandomStringUtils.randomAlphanumeric(stringLength),
                    )
                }
                this.client.insert(batch)
                this.client.commit(txId)
            }
            this.client.optimize(OptimizeEntity(entityName.fqn))
        }
        Assertions.assertEquals(repeatBatchInsert * batchCount, countElements(this.client, entityName)!!.toInt())
    }
}