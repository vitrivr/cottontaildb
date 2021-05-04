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
import org.vitrivr.cottontail.client.stub.SimpleClient
import org.vitrivr.cottontail.embedded
import kotlin.time.ExperimentalTime

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DDLServiceTest {

    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel
    private final var TEST_ENTITY_FQN_INPUT = "$TEST_SCHEMA.$TEST_ENTITY"
    private final var TEST_ENTITY_FQN_OUTPUT = "warren.$TEST_SCHEMA.$TEST_ENTITY"

    @ExperimentalTime
    @BeforeAll
    fun startCottontail() {
        embedded(TestConstants.testConfig())
    }

    @BeforeEach
    fun setup() {
        val builder = NettyChannelBuilder.forAddress("localhost", 1865)
        builder.usePlaintext()
        this.channel = builder.build()
        this.client = SimpleClient(this.channel)
        assert(client.ping())
        dropTestSchema()
    }

    @AfterEach
    fun tearDown() {
        dropTestSchema()
    }

    fun dropTestSchema() {
        /* Teardown */
        try {
            client.drop(DropSchema(TEST_SCHEMA))
        } catch (e: Exception) {
            //ignore
        }
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
        createTestSchema()
        val names = schemaNames()
        assert(names.contains("warren.$TEST_SCHEMA")) { "returned schema names were $names instead of expected $TEST_SCHEMA" }
    }

    @Test
    fun createAndDropSchema() {
        createTestSchema()
        client.drop(DropSchema(TEST_SCHEMA))
        assert(!schemaNames().contains(TEST_SCHEMA)) { "schema $TEST_SCHEMA was not dropped" }
    }

    @Test
    fun dropNonExistingEntity() {
        createTestSchema()
        try {
            client.drop(DropEntity(TEST_ENTITY_FQN_INPUT))
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.NOT_FOUND.code) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        }
    }

    @Test
    fun createAndListEntity() {
        createAndListEntity(TEST_ENTITY_FQN_INPUT)
        dropTestSchema()
        createAndListEntity(TEST_ENTITY_FQN_OUTPUT)
    }

    fun createAndListEntity(input: String) {
        createTestSchema()
        createTestEntity(input)
        val names = entityNames()
        assert(names.contains(TEST_ENTITY_FQN_OUTPUT)) { "returned schema names were $names instead of expected $TEST_ENTITY_FQN_OUTPUT" }
    }

    @Test
    fun createAndVerifyAboutEntity() {
        createTestSchema()
        createTestEntity()
        val about = client.about(AboutEntity(TEST_ENTITY_FQN_INPUT))
        assert(about.hasNext()) { "could not verify existence with about message" }
    }

    /**
     * drop entity api should accept both with and without warren qualifier
     */
    @Test
    fun createAndDropEntity() {
        createAndDropEntity(TEST_ENTITY_FQN_INPUT)
        dropTestSchema()
        createAndDropEntity(TEST_ENTITY_FQN_OUTPUT)
    }

    fun createAndDropEntity(input: String) {
        createTestSchema()
        createTestEntity()
        client.drop(DropEntity(TEST_ENTITY_FQN_INPUT))
        assert(!entityNames().contains(TEST_ENTITY_FQN_OUTPUT)) { "entity $TEST_ENTITY was not dropped" }
    }


    private fun createTestSchema() {
        client.create(CreateSchema(TEST_SCHEMA))
    }

    private fun createTestEntity(input: String = TEST_ENTITY_FQN_INPUT) {
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
