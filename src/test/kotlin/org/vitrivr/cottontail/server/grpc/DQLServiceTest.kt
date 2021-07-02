package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.extensions.And
import org.vitrivr.cottontail.client.language.extensions.Literal
import org.vitrivr.cottontail.client.stub.SimpleClient
import org.vitrivr.cottontail.embedded
import kotlin.random.Random
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DQLServiceTest {

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
        GrpcTestUtils.dropTestSchema(client)
        GrpcTestUtils.createTestSchema(client)
        GrpcTestUtils. createTestVectorEntity(client)
        GrpcTestUtils.createTestEntity(client)
        GrpcTestUtils.populateTestEntity(client)
        GrpcTestUtils.populateVectorEntity(client)
    }

    @AfterAll
    fun cleanup() {
        GrpcTestUtils.dropTestSchema(client)
        this.embedded.stop()
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
        val countQuery = Query(GrpcTestUtils.TEST_ENTITY_FQN_INPUT).count()
        val count = client.query(countQuery).next()
        assert(count.asLong(0)!! == GrpcTestUtils.TEST_ENTITY_TUPLE_COUNT)
    }

    @Test
    fun queryColumn() {
        val query = Query().from(GrpcTestUtils.TEST_ENTITY_FQN_INPUT).select(GrpcTestUtils.STRING_COLUMN_NAME)
        val result = client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(GrpcTestUtils.STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun queryBetween() {
        val random = Random.Default
        val lb = random.nextInt(98)
        val ub = random.nextInt(lb+1, 100)
        val query = Query().from(GrpcTestUtils.TEST_ENTITY_FQN_INPUT)
            .select(GrpcTestUtils.STRING_COLUMN_NAME, GrpcTestUtils.INT_COLUMN_NAME)
            .where(Literal(GrpcTestUtils.INT_COLUMN_NAME, "BETWEEN", lb, ub))
        val result = client.query(query)
        for (el in result) {
            assert(el.asInt(GrpcTestUtils.INT_COLUMN_NAME)!! in lb..ub)
        }
    }

    @Test
    fun queryBetweenWithAnd() {
        val random = Random.Default
        val lb = random.nextInt(50)
        val query = Query().from(GrpcTestUtils.TEST_ENTITY_FQN_INPUT)
            .select(GrpcTestUtils.STRING_COLUMN_NAME, GrpcTestUtils.INT_COLUMN_NAME)
            .where(
                 And(
                     Literal(GrpcTestUtils.INT_COLUMN_NAME, "BETWEEN", lb, lb+1),
                     Literal(GrpcTestUtils.INT_COLUMN_NAME, "BETWEEN", lb+1, lb+2)
                )
            )
        val result = client.query(query)
        for (el in result) {
            assert(el.asInt(GrpcTestUtils.INT_COLUMN_NAME)!! == lb + 1)
        }
    }

    @Test
    fun queryColumnWithVector() {
        val query = Query().from(GrpcTestUtils.TEST_VECTOR_ENTITY_FQN_INPUT).select(GrpcTestUtils.STRING_COLUMN_NAME)
        val result = client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(GrpcTestUtils.STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun haversineDistance() {
        val query = Query().from(GrpcTestUtils.TEST_VECTOR_ENTITY_FQN_INPUT).knn(GrpcTestUtils.TWOD_COLUMN_NAME, 2, "haversine", arrayOf(5f, 10f))
        val result = client.query(query)
        val el = result.next()
        val distance = el.asDouble("distance")
        assert(distance != null)
    }
}
