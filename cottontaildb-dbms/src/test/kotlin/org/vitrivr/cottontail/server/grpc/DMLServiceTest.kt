package org.vitrivr.cottontail.server.grpc

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
import org.vitrivr.cottontail.test.GrpcTestUtils.INT_COLUMN_NAME
import org.vitrivr.cottontail.test.GrpcTestUtils.STRING_COLUMN_NAME
import org.vitrivr.cottontail.test.GrpcTestUtils.TEST_ENTITY_FQN
import org.vitrivr.cottontail.test.GrpcTestUtils.TEST_ENTITY_TUPLE_COUNT
import org.vitrivr.cottontail.test.GrpcTestUtils.TEST_VECTOR_ENTITY_FQN_INPUT
import org.vitrivr.cottontail.test.GrpcTestUtils.TWOD_COLUMN_NAME
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.math.random.nextInt
import kotlin.time.ExperimentalTime

/**
 * Integration tests that test the DML endpoint of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class DMLServiceTest : AbstractClientTest() {

    @BeforeEach
    fun beforeEach() {
        this.startAndPopulateCottontail()
    }

    @AfterEach
    fun afterEach() {
        this.cleanup()
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
        val insert1 = Insert(TEST_VECTOR_ENTITY_FQN_INPUT).values(
            STRING_COLUMN_NAME to string1,
            INT_COLUMN_NAME to random.nextInt(0, 10),
            TWOD_COLUMN_NAME to floatArrayOf(0.0f, 0.0f)
        ).txId(tx1)
        this.client.insert(insert1)

        /* Check results; insert 1 should exist. */
        val result1 = this.client.query(Query(TEST_VECTOR_ENTITY_FQN_INPUT).where(Expression(STRING_COLUMN_NAME, "=", string1)).count())
        Assertions.assertEquals(0L, result1.next().asLong(0))

        /* Execute commits and check. */
        Assertions.assertDoesNotThrow { this.client.commit(tx1) }

        /* Check results; insert 2 should not exist. */
        val result2 = this.client.query(Query(TEST_VECTOR_ENTITY_FQN_INPUT).where(Expression(STRING_COLUMN_NAME, "=", string1)).count())
        Assertions.assertEquals(1L, result2.next().asLong(0))
    }

    @Test
    fun testUpdateAllColumns() {
        val newValue = RandomStringUtils.randomAlphabetic(4)

        /* Perform update and sanity checks. */
        val update = Update().from(TEST_ENTITY_FQN).values(Pair(STRING_COLUMN_NAME, newValue))
        val r1 = this.client.update(update)
        Assertions.assertTrue(r1.hasNext())
        val el1 = r1.next()
        Assertions.assertEquals(TEST_ENTITY_TUPLE_COUNT, el1.asLong(0))

        /* Query and check values. */
        val select = Query().select("*").from(TEST_ENTITY_FQN)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertEquals(newValue, el2.asString(STRING_COLUMN_NAME))
        }
    }

    @Test
    fun testUpdateAllColumnsWithCommitAndQuery() {
        /* Query and update values. */
        val txId = this.client.begin()
        val s1 = Query().select("*").from(TEST_ENTITY_FQN).txId(txId)
        val r1 = this.client.query(s1)
        for (el1 in r1) {
            val update = Update()
                .from(TEST_ENTITY_FQN)
                .values(Pair(INT_COLUMN_NAME, -1))
                .where(Expression(STRING_COLUMN_NAME, "=", el1.asString(STRING_COLUMN_NAME)!!))
                .txId(txId)
            val r2 = this.client.update(update)
            Assertions.assertTrue(r2.hasNext())
            val el2 = r2.next()
            Assertions.assertEquals(1, el2.asLong(0))
        }
        this.client.commit(txId)

        /* Query and check values. */
        val select = Query().select("*").from(TEST_ENTITY_FQN)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertEquals(-1, el2.asInt(INT_COLUMN_NAME))
        }
    }

    @Test
    fun testUpdateAllColumnsWithRollbackAndQuery() {
        /* Query and update values. */
        val txId = this.client.begin()
        val s1 = Query().select("*").from(TEST_ENTITY_FQN).txId(txId)
        val r1 = this.client.query(s1)
        for (el1 in r1) {
            val update = Update().from(TEST_ENTITY_FQN)
                .values(Pair(INT_COLUMN_NAME, -1))
                .where(Expression(STRING_COLUMN_NAME, "=", el1.asString(STRING_COLUMN_NAME)!!))
                .txId(txId)
            val r2 = this.client.update(update)
            Assertions.assertTrue(r2.hasNext())
            val el2 = r2.next()
            Assertions.assertEquals(1, el2.asLong(0))
        }
        this.client.rollback(txId)

        /* Query and check values. */
        val select = Query().select("*").from(TEST_ENTITY_FQN)
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
        val en = "lucenetest"
        val entityFqn = "${TestConstants.TEST_SCHEMA}.$en"
        val batchCount = 10000
        val repeatBatchInsert = 100
        val stringLength = 200
        var txId = client.begin()
        val create = CreateEntity(entityFqn).column(STRING_COLUMN_NAME, Type.STRING)
        client.create(create)
        client.create(CreateIndex(entityFqn, STRING_COLUMN_NAME, CottontailGrpc.IndexType.LUCENE))
        client.commit(txId)
        // we have an outer loop to check if optimization is the problem.
        // The first inserts are all made without optimization in between, and between the first and the second is an optimize
        repeat(10) {
            repeat(repeatBatchInsert / 10) {
                txId = client.begin()
                val batch = BatchInsert().into(entityFqn).columns(STRING_COLUMN_NAME)
                repeat(batchCount) {
                    batch.append(
                        RandomStringUtils.randomAlphanumeric(stringLength),
                    )
                }
                client.insert(batch)
                client.commit(txId)
            }
            client.optimize(OptimizeEntity(entityFqn))
        }
        Assertions.assertEquals(repeatBatchInsert * batchCount, countElements(client, en)!!.toInt())
    }
}