package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.embedded
import org.vitrivr.cottontail.server.CottontailServer
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.test.TestConstants.DBO_CONSTANT
import org.vitrivr.cottontail.test.TestConstants.TEST_SCHEMA
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DDLServiceTest {

    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel
    private lateinit var embedded: CottontailServer

    @BeforeAll
    fun startCottontail() {
        this.embedded = embedded(TestConstants.testConfig())
        this.channel =  NettyChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()
        this.client = SimpleClient(this.channel)

        /** Drop and (re-)create test schema. */
        GrpcTestUtils.dropTestSchema(client)
    }

    @AfterAll
    fun cleanup() {
        /* Drop test schema. */
        GrpcTestUtils.dropTestSchema(client)

        /** Close SimpleClient. */
        this.client.close()

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(25000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.shutdownAndWait()

    }

    @BeforeEach
    fun setup() {
        assert(client.ping())
    }

    @Test
    fun pingTest() {
        assert(client.ping()) { "ping unsuccessful" }
    }

    /**
     * At this point, the test schema should not exist.
     * Therefore, trying to drop it should lead to a NOT_FOUND StatusRuntimeException
     */
    @Test
    fun dropNotFoundSchema() {
        try {
            client.drop(DropSchema(TEST_SCHEMA))
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.NOT_FOUND.code) {
                fail("Status was " + e.status + " instead of NOT_FOUND")
            }

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndListSchema() {
        try {
            client.create(CreateSchema(TEST_SCHEMA))
            val names = schemaNames()
            assert(names.contains("warren.$TEST_SCHEMA")) { "returned schema names were $names instead of expected $TEST_SCHEMA" }
        } finally {
            client.drop(DropSchema(TEST_SCHEMA))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndDropSchema() {
        this.client.create(CreateSchema(TEST_SCHEMA))
        this.client.drop(DropSchema(TEST_SCHEMA))
        val names = schemaNames()
        assert(!names.contains("warren.$TEST_SCHEMA")) { "Schema $TEST_SCHEMA was not dropped" }

        /* Make sure, that no transaction or locks are dangling. */
        val locks = this.client.locks()
        Assertions.assertFalse(locks.hasNext())
        locks.close()
    }

    @Test
    fun dropNonExistingEntity() {
        try {
            client.create(CreateSchema(TEST_SCHEMA))
            client.drop(DropEntity(GrpcTestUtils.TEST_ENTITY_FQN))
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.NOT_FOUND.code) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        } finally {
            client.drop(DropSchema(TEST_SCHEMA))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndListEntity() {
        try {
            client.create(CreateSchema(TEST_SCHEMA))
            client.create(CreateEntity(GrpcTestUtils.TEST_ENTITY_FQN).column("id", Type.STRING))
            val names = entityNames()
            assert(names.contains(GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN)) { "Returned entity names do not contain ${GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN}." }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TEST_SCHEMA))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndVerifyAboutEntity() {
        try {
            this.client.create(CreateSchema(TEST_SCHEMA))
            this.client.create(CreateEntity(GrpcTestUtils.TEST_ENTITY_FQN).column("id", Type.STRING))
            val about = this.client.about(AboutEntity(GrpcTestUtils.TEST_ENTITY_FQN))
            assert(about.hasNext()) { "could not verify existence with about message" }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TEST_SCHEMA))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    /**
     * drop entity api should accept both with and without warren qualifier
     */
    @Test
    fun createAndDropEntity() {
        try {
            client.create(CreateSchema(TEST_SCHEMA))
            client.create(CreateEntity(GrpcTestUtils.TEST_ENTITY_FQN).column("id", Type.STRING))
            client.drop(DropEntity(GrpcTestUtils.TEST_ENTITY_FQN))
            val names = entityNames()
            assert(!names.contains(GrpcTestUtils.TEST_ENTITY_FQN)) { "Returned entity names do not contain ${GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN}." }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${GrpcTestUtils.TEST_ENTITY_FQN} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TEST_SCHEMA))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    /**
     * drop entity api should accept both with and without warren qualifier
     */
    @Test
    fun createAndDropEntityWithWarren() {
        try {
            client.create(CreateSchema(TEST_SCHEMA))
            client.create(CreateEntity(GrpcTestUtils.TEST_ENTITY_FQN).column("id", Type.STRING))
            client.drop(DropEntity(GrpcTestUtils.TEST_ENTITY_FQN))
            val names = entityNames()
            assert(!names.contains(GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN)) { "Returned entity names do not contain ${GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN}." }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${GrpcTestUtils.TEST_ENTITY_FQN_WITH_WARREN} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TEST_SCHEMA))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun aboutNonExistingEntity() {
        Assertions.assertThrows(StatusRuntimeException::class.java) {
            this.client.about(AboutEntity(GrpcTestUtils.TEST_ENTITY_FQN))
        }

        /* Make sure, that no transaction or locks are dangling. */
        val locks = this.client.locks()
        Assertions.assertFalse(locks.hasNext())

        /* Close iterator. */
        locks.close()
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
