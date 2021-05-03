package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.TestConstants.DBO_CONSTANT
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY_FULL
import org.vitrivr.cottontail.TestConstants.TEST_SCHEMA
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.stub.SimpleClient
import org.vitrivr.cottontail.embedded
import kotlin.time.ExperimentalTime

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DDLServiceTest {

    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel

    @ExperimentalTime
    @BeforeAll
    fun startCottontail() {
        embedded(TestConstants.testConfig());
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
            if (e.status != Status.NOT_FOUND) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        }
    }

    fun createTestSchema() {
        client.create(CreateSchema(TEST_SCHEMA))
    }

    fun createTestEntity() {
        client.create(CreateEntity(TEST_ENTITY_FULL))
    }

    fun schemaNames(): List<String> {
        val names = mutableListOf<String>()
        client.list(ListSchemas()).forEach { t -> names.add(t.asString(DBO_CONSTANT)!!) }
        return names
    }

    fun entityNames(): List<String> {
        val names = mutableListOf<String>()
        client.list(ListEntities(TEST_SCHEMA)).forEach { t -> names.add(t.asString(DBO_CONSTANT)!!) }
        return names
    }

    @Test
    fun createAndListSchema() {
        createTestSchema()
        val names = schemaNames()
        assert(names.contains(TEST_SCHEMA)) { "returned schema names were $names instead of expected $TEST_SCHEMA" }
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
            client.drop(DropEntity(TEST_ENTITY_FULL))
        } catch (e: StatusRuntimeException) {
            if (e.status != Status.NOT_FOUND) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        }
    }

    @Test
    fun createAndListEntity() {
        createTestSchema()
        createTestEntity()
        val names = entityNames()
        assert(names.contains(TEST_ENTITY_FULL)) { "returned schema names were $names instead of expected $TEST_ENTITY_FULL" }
    }

    @Test
    fun createAndDropEntity() {
        createTestSchema()
        createTestEntity()
        client.drop(DropEntity(TEST_ENTITY_FULL))
        assert(!entityNames().contains(TEST_ENTITY_FULL)) { "entity $TEST_ENTITY_FULL was not dropped" }
    }


}