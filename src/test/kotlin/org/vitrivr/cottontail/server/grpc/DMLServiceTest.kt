package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY_TUPLE_COUNT
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.extensions.Literal
import org.vitrivr.cottontail.client.stub.SimpleClient
import org.vitrivr.cottontail.embedded
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DMLServiceTest {
    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel
    private lateinit var embedded: CottontailGrpcServer


    @BeforeAll
    fun startCottontail() {
        this.embedded = embedded(TestConstants.testConfig())
        val builder = NettyChannelBuilder.forAddress("localhost", 1865)
        builder.usePlaintext()
        this.channel = builder.build()
        this.client = SimpleClient(this.channel)
        assert(client.ping())
        dropTestSchema(client)
        createTestSchema(client)
        createTestVectorEntity(client)
        createTestEntity(client)
        populateTestEntity(client)
        populateVectorEntity(client)
    }

    @Test
    fun testUpdateAllColumns() {
        val newValue = RandomStringUtils.randomAlphabetic(4)

        /* Perform update and sanity checks. */
        val update = Update().from(TestConstants.TEST_ENTITY_FQN_INPUT).values(Pair(TestConstants.STRING_COLUMN_NAME, newValue))
        val r1 = this.client.update(update)
        Assertions.assertTrue(r1.hasNext())
        val el1 = r1.next()
        Assertions.assertEquals(TEST_ENTITY_TUPLE_COUNT, el1.asLong(0))

        /* Query and check values. */
        val select = Query().select("*").from(TestConstants.TEST_ENTITY_FQN_INPUT)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertEquals(newValue, el2.asString(TestConstants.STRING_COLUMN_NAME))
        }
    }

    @Test
    fun testUpdateAllColumnsWithCommitAndQuery() {
        /* Query and update values. */
        val txId = this.client.begin()
        val s1 = Query().select("*").from(TestConstants.TEST_ENTITY_FQN_INPUT)
        val r1 = this.client.query(s1, txId)
        for (el1 in r1) {
            val update = Update().from(TestConstants.TEST_ENTITY_FQN_INPUT).values(Pair(TestConstants.INT_COLUMN_NAME, -1)).where(Literal(TestConstants.STRING_COLUMN_NAME, "=", el1.asString(TestConstants.STRING_COLUMN_NAME)!!))
            val r2 = this.client.update(update, txId)
            Assertions.assertTrue(r2.hasNext())
            val el2 = r2.next()
            Assertions.assertEquals(1, el2.asLong(0))
        }
        this.client.commit(txId)

        /* Query and check values. */
        val select = Query().select("*").from(TestConstants.TEST_ENTITY_FQN_INPUT)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertEquals(-1, el2.asInt(TestConstants.INT_COLUMN_NAME))
        }
    }

    @Test
    fun testUpdateAllColumnsWithRollbackAndQuery() {
        /* Query and update values. */
        val txId = this.client.begin()
        val s1 = Query().select("*").from(TestConstants.TEST_ENTITY_FQN_INPUT)
        val r1 = this.client.query(s1, txId)
        for (el1 in r1) {
            val update = Update().from(TestConstants.TEST_ENTITY_FQN_INPUT).values(Pair(TestConstants.INT_COLUMN_NAME, -1)).where(Literal(TestConstants.STRING_COLUMN_NAME, "=", el1.asString(TestConstants.STRING_COLUMN_NAME)!!))
            val r2 = this.client.update(update, txId)
            Assertions.assertTrue(r2.hasNext())
            val el2 = r2.next()
            Assertions.assertEquals(1, el2.asLong(0))
        }
        this.client.rollback(txId)

        /* Query and check values. */
        val select = Query().select("*").from(TestConstants.TEST_ENTITY_FQN_INPUT)
        val r2 = this.client.query(select)
        for (el2 in r2) {
            Assertions.assertNotEquals(-1, el2.asInt(TestConstants.INT_COLUMN_NAME))
        }
    }

    @AfterAll
    fun cleanup() {
        dropTestSchema(this.client)

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(5000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.stop()
    }

    @BeforeEach
    fun setup() {
        assert(client.ping())
    }
}