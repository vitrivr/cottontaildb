package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.TestConstants.DBO_CONSTANT
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY
import org.vitrivr.cottontail.TestConstants.TEST_SCHEMA
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.embedded
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DDLServiceTest {

    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel
    private lateinit var embedded: CottontailGrpcServer

    @BeforeAll
    fun startCottontail() {
        this.embedded = embedded(TestConstants.testConfig())
        this.channel =  NettyChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()
        this.client = SimpleClient(this.channel)
    }

    @AfterAll
    fun cleanup() {
        /** Close SimpleClient. */
        this.client.close()

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(25000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.stop()
    }

    @BeforeEach
    fun setup() {
        assert(client.ping())
        GrpcTestUtils.dropTestSchema(client)
    }

    @AfterEach
    fun tearDown() {
        GrpcTestUtils.dropTestSchema(client)
    }


    @Test
    fun pingTest() {
        assert(client.ping()) { "ping unsuccessful" }
    }

    /**
     * At this point, the test schema should not exists.
     * Therefore, trying to drop it should lead to a NOT_FOUND StatusRuntimeException
     */
    @Test
    fun dropNotFoundSchema() {
        try {
            client.drop(DropSchema(TEST_SCHEMA))
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.NOT_FOUND.code) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        }
    }

    @Test
    fun createAndListSchema() {
        GrpcTestUtils.createTestSchema(client)
        val names = schemaNames()
        assert(names.contains("warren.$TEST_SCHEMA")) { "returned schema names were $names instead of expected $TEST_SCHEMA" }
    }

    @Test
    fun createAndDropSchema() {
        GrpcTestUtils.createTestSchema(client)
        client.drop(DropSchema(TEST_SCHEMA))
        assert(!schemaNames().contains(TEST_SCHEMA)) { "schema $TEST_SCHEMA was not dropped" }
    }

    @Test
    fun dropNonExistingEntity() {
        GrpcTestUtils.createTestSchema(client)
        try {
            client.drop(DropEntity(GrpcTestUtils.TEST_ENTITY_FQN_INPUT))
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.NOT_FOUND.code) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        }
    }

    @Test
    fun createAndListEntity() {
        createAndListEntity(GrpcTestUtils.TEST_ENTITY_FQN_INPUT)
        GrpcTestUtils.dropTestSchema(client)
        createAndListEntity(GrpcTestUtils.TEST_ENTITY_FQN_OUTPUT)
    }

    fun createAndListEntity(input: String) {
        GrpcTestUtils.createTestSchema(client)
        createTestEntity(input)
        val names = entityNames()
        assert(names.contains(GrpcTestUtils.TEST_ENTITY_FQN_OUTPUT)) { "returned schema names were $names instead of expected ${GrpcTestUtils.TEST_ENTITY_FQN_OUTPUT}" }
    }

    @Test
    fun createAndVerifyAboutEntity() {
        GrpcTestUtils.createTestSchema(client)
        createTestEntity()
        val about = client.about(AboutEntity(GrpcTestUtils.TEST_ENTITY_FQN_INPUT))
        assert(about.hasNext()) { "could not verify existence with about message" }
    }

    /**
     * drop entity api should accept both with and without warren qualifier
     */
    @Test
    fun createAndDropEntity() {
        createAndDropEntity(GrpcTestUtils.TEST_ENTITY_FQN_INPUT)
        GrpcTestUtils.dropTestSchema(client)
        createAndDropEntity(GrpcTestUtils.TEST_ENTITY_FQN_OUTPUT)
    }

    fun createAndDropEntity(input: String) {
        GrpcTestUtils.createTestSchema(client)
        createTestEntity()
        client.drop(DropEntity(input))
        assert(!entityNames().contains(GrpcTestUtils.TEST_ENTITY_FQN_OUTPUT)) { "entity $TEST_ENTITY was not dropped" }
    }

    private fun createTestEntity(input: String = GrpcTestUtils.TEST_ENTITY_FQN_INPUT) {
        client.create(CreateEntity(input))
    }

    private fun schemaNames(): List<String> {
        val names = mutableListOf<String>()
        client.list(ListSchemas()).forEach { t -> names.add(t.asString(DBO_CONSTANT)!!) }
        return names
    }

    private fun entityNames(): List<String> {
        val names = mutableListOf<String>()
        client.list(ListEntities(TEST_SCHEMA)).forEach { t -> names.add(t.asString(DBO_CONSTANT)!!) }
        return names
    }
}
