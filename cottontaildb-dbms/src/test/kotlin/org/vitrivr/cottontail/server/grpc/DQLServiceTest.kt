package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.embedded
import org.vitrivr.cottontail.server.CottontailServer
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.test.TestConstants.STRING_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.TEST_ENTITY_NAME
import org.vitrivr.cottontail.test.TestConstants.TEST_VECTOR_ENTITY_NAME
import org.vitrivr.cottontail.test.TestConstants.TWOD_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.testConfig
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

/**
 * Integration tests that test the DQL endpoint of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DQLServiceTest {

    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel
    private lateinit var embedded: CottontailServer

    @BeforeAll
    fun startCottontail() {
        this.embedded = embedded(testConfig())
        val builder = NettyChannelBuilder.forAddress("localhost", 1865)
        builder.usePlaintext() /* */
        this.channel = builder.build()
        this.client = SimpleClient(this.channel)
        assert(client.ping())
        GrpcTestUtils.dropTestSchema(client)
        GrpcTestUtils.createTestSchema(client)
        GrpcTestUtils.createTestVectorEntity(client)
        GrpcTestUtils.createTestEntity(client)
        GrpcTestUtils.populateTestEntity(client)
        GrpcTestUtils.populateVectorEntity(client)
    }

    @AfterAll
    fun cleanup() {
        GrpcTestUtils.dropTestSchema(client)

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(5000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.shutdownAndWait()
    }

    @BeforeEach
    fun setup() {
        assert(client.ping())
    }

    @AfterEach
    fun tearDown() {
        assert(client.ping())
    }

    @Test
    fun pingTest() {
        assert(client.ping()) { "ping unsuccessful" }
    }

    @Test
    fun count() {
        val count = GrpcTestUtils.countElements(this.client, TEST_ENTITY_NAME)
        assertEquals(TestConstants.TEST_COLLECTION_SIZE.toLong(), count)

        /* Now scan the same entity; count should be the same. */
        val scanQuery = Query(TEST_ENTITY_NAME.fqn).select("*")
        var bruteForceCount = 0L
        this.client.query(scanQuery).forEachRemaining {
            bruteForceCount += 1L
        }
        assertEquals(bruteForceCount, count)
    }

    @Test
    fun queryColumn() {
        val query = Query().from(TEST_ENTITY_NAME.fqn).select(STRING_COLUMN_NAME)
        val result = client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun queryColumnWithVector() {
        val query = Query().from(TEST_VECTOR_ENTITY_NAME.fqn).select(STRING_COLUMN_NAME)
        val result = client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun haversineDistance() {
        val query = Query()
                .select("*")
                .from(TEST_VECTOR_ENTITY_NAME.fqn)
                .distance(TWOD_COLUMN_NAME, arrayOf(5f, 10f), Distances.HAVERSINE, "distance")
                .order("distance", Direction.ASC)
                .limit(500)
        val result = client.query(query)
        val el = result.next()
        val distance = el.asDouble("distance")
        assert(distance != null)
    }

    @Test
    fun queryNNSWithLikeStart() {
        val query = Query()
                .select("*")
                .from(TEST_VECTOR_ENTITY_NAME.fqn)
                .distance(TWOD_COLUMN_NAME, arrayOf(5f, 10f), Distances.L2, "distance")
                .where(Expression(STRING_COLUMN_NAME, "LIKE", "a%"))
                .order("distance", Direction.ASC)
                .limit(500)

        val result = client.query(query)
        for (r in result) {
            val distance = r.asDouble("distance")
            val string = r.asString(STRING_COLUMN_NAME)!!
            assert(distance != null)
            assertTrue(string.first() == 'a')
        }
    }

    @Test
    fun queryNNSWithLikeEnd() {
        val query = Query().from(TEST_VECTOR_ENTITY_NAME.fqn)
                .select("*")
                .distance(TWOD_COLUMN_NAME, arrayOf(5f, 10f), Distances.L2, "distance")
                .where(Expression(STRING_COLUMN_NAME, "LIKE", "%z"))
                .order("distance", Direction.ASC)
                .limit(500)
        val result = client.query(query)
        for (r in result) {
            val distance = r.asDouble("distance")
            val string = r.asString(STRING_COLUMN_NAME)!!
            assert(distance != null)
            assertTrue(string.last() == 'z')
        }
    }

    @Test
    fun distinctLookup() {
        val entryStrings = listOf("one", "ONE", "two", "three",)
        testDistinct(entryStrings)
    }

    @Test
    fun distinctLookupWithNull() {
        val entryStrings = listOf("one", "ONE", "two", "three", "NULL", "null")
        testDistinct(entryStrings)
    }

    private fun testDistinct(entryStrings: List<String>){
        val entityName = TestConstants.TEST_SCHEMA.entity("test")
        var txId = client.begin()
        //create entity with one column
        this.client.create(CreateEntity(entityName.fqn).column(STRING_COLUMN_NAME, Type.STRING).txId(txId))
        this.client.commit(txId)

        //insert the same entry multiple times
        txId = this.client.begin()
        entryStrings.forEachIndexed { idx, s ->
            repeat(maxOf(2, idx)) {
                val insert = Insert().into(entityName.fqn).value(STRING_COLUMN_NAME, s).txId(txId)
                this.client.insert(insert)
            }
        }
        this.client.commit(txId)
        val query = Query().from(entityName.fqn)
                .distinct(STRING_COLUMN_NAME, null)
        val result = this.client.query(query)
        val set = mutableSetOf<String>()
        for(r in result){
            val string = r.asString(STRING_COLUMN_NAME)!!
            assertTrue(set.add(string), "$string was returned twice!")
        }
        entryStrings.forEach { s -> assertTrue(set.contains(s), "$s was not returned") }
    }
}
