package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.predicate.Compare
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.DropEntity
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.values.FloatVectorValue
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
        val query = Query(TEST_ENTITY_NAME).select(STRING_COLUMN_NAME)
        val result = this.client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun queryColumnWithVector() {
        val query = Query(TEST_VECTOR_ENTITY_NAME).select(STRING_COLUMN_NAME)
        val result = this.client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun haversineDistance() {
        val query = Query(TEST_VECTOR_ENTITY_NAME)
                .select("*")
                .distance(TWOD_COLUMN_NAME, FloatVectorValue(arrayOf(5f, 10f)), Distances.HAVERSINE, "distance")
                .order("distance", Direction.ASC)
                .limit(500)
        val result = client.query(query)
        val el = result.next()
        val distance = el.asDouble("distance")
        assert(distance != null)
    }

    @Test
    fun queryNNSWithLikeStart() {
        val query = Query(TEST_VECTOR_ENTITY_NAME)
                .select("*")
                .distance(TWOD_COLUMN_NAME, FloatVectorValue(arrayOf(5f, 10f)), Distances.L2, "distance")
                .where(Compare(STRING_COLUMN_NAME, "LIKE", "a%"))
                .order("distance", Direction.ASC)
                .limit(500)

        val result = this.client.query(query)
        for (r in result) {
            val distance = r.asDouble("distance")
            val string = r.asString(STRING_COLUMN_NAME)!!
            assert(distance != null)
            assertTrue(string.first() == 'a')
        }
    }

    @Test
    fun queryNNSWithLikeEnd() {
        val query = Query(TEST_VECTOR_ENTITY_NAME)
                .select("*")
                .distance(TWOD_COLUMN_NAME, FloatVectorValue(arrayOf(5f, 10f)), Distances.L2, "distance")
                .where(Compare(STRING_COLUMN_NAME, "LIKE", "%z"))
                .order("distance", Direction.ASC)
                .limit(500)
        val result = this.client.query(query)
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
        /* Create entity with one column. */
        val entityName = TestConstants.TEST_SCHEMA.entity("distinct_test")
        this.client.create(CreateEntity(entityName.fqn).column(STRING_COLUMN_NAME, "STRING"))

        try {
            /* INSERT the same entry multiple times. */
            val txId = this.client.begin()
            entryStrings.forEachIndexed { idx, s ->
                repeat(maxOf(2, idx)) {
                    val insert = Insert(entityName).any(STRING_COLUMN_NAME, s).txId(txId)
                    this.client.insert(insert)
                }
            }
            this.client.commit(txId)

            /* Execute and check query. */
            val query = Query(entityName).distinct(STRING_COLUMN_NAME, null)
            val result = this.client.query(query)
            val set = mutableSetOf<String>()
            for(r in result){
                val string = r.asString(STRING_COLUMN_NAME)!!
                assertTrue(set.add(string), "$string was returned twice!")
            }
            entryStrings.forEach { s -> assertTrue(set.contains(s), "$s was not returned") }
        } finally {
            this.client.drop(DropEntity(entityName.fqn))
        }
    }
}
