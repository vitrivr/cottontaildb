package org.vitrivr.cottontail.server.grpc

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.test.TestConstants.DBO_CONSTANT
import kotlin.time.ExperimentalTime

/**
 * Integration tests that test the DDL endpoint of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DDLServiceTest : AbstractClientTest() {

    @BeforeEach
    fun beforeEach() {
        this.cleanAndConnect()
    }

    @AfterEach
    fun afterEach() {
        GrpcTestUtils.dropTestSchema(this.client)
    }

    @AfterAll
    fun afterAll() {
        this.cleanup()
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
    fun truncateEntity() {
        /* Prepare and populate test entity. */
        GrpcTestUtils.createTestSchema(client)
        GrpcTestUtils.createTestEntity(client)
        GrpcTestUtils.populateTestEntity(client)

        /* Check count before TRUNCATE. */
        Assertions.assertEquals(TestConstants.TEST_COLLECTION_SIZE.toLong(), GrpcTestUtils.countElements(client, TestConstants.TEST_ENTITY_NAME))

        /* Execute TRUNCATE. */
        this.client.truncate(TruncateEntity(TestConstants.TEST_ENTITY_NAME.fqn))

        /* Check count after TRUNCATE. */
        Assertions.assertEquals(0, GrpcTestUtils.countElements(client, TestConstants.TEST_ENTITY_NAME))
    }

    @Test
    fun truncateEntityWithLucene() {
        /* Prepare and populate test entity. */
        GrpcTestUtils.createTestSchema(client)
        GrpcTestUtils.createTestEntity(client)
        GrpcTestUtils.populateTestEntity(client)

        /* Check count before TRUNCATE. */
        Assertions.assertEquals(TestConstants.TEST_COLLECTION_SIZE.toLong(), GrpcTestUtils.countElements(client, TestConstants.TEST_ENTITY_NAME))
        GrpcTestUtils.createLuceneIndexOnTestEntity(this.client)

        /* Execute TRUNCATE. */
        this.client.truncate(TruncateEntity(TestConstants.TEST_ENTITY_NAME.fqn))

        /* Check count after TRUNCATE. */
        Assertions.assertEquals(0, GrpcTestUtils.countElements(client, TestConstants.TEST_ENTITY_NAME))
    }

    @Test
    fun dropNotFoundSchema() {
        try {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))
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
            client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
            val names = schemaNames()
            assert(names.contains(TestConstants.TEST_SCHEMA.fqn)) { "returned schema names were $names instead of expected ${TestConstants.TEST_SCHEMA}" }
        } finally {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndDropSchema() {
        this.client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
        this.client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))
        val names = schemaNames()
        assert(!names.contains("${TestConstants.TEST_SCHEMA}")) { "Schema ${TestConstants.TEST_SCHEMA} was not dropped" }

        /* Make sure, that no transaction or locks are dangling. */
        val locks = this.client.locks()
        Assertions.assertFalse(locks.hasNext())
        locks.close()
    }

    @Test
    fun dropNonExistingEntity() {
        try {
            client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
            client.drop(DropEntity(TestConstants.TEST_ENTITY_NAME.fqn))
        } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.NOT_FOUND.code) {
                fail("status was " + e.status + " instead of NOT_FOUND")
            }
        } finally {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndListEntity() {
        try {
            client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
            client.create(CreateEntity(TestConstants.TEST_ENTITY_NAME.fqn).column("id", "STRING"))
            val names = entityNames()
            assert(names.contains(TestConstants.TEST_ENTITY_NAME.fqn)) { "Returned entity names do not contain ${TestConstants.TEST_ENTITY_NAME.fqn}." }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${TestConstants.TEST_ENTITY_NAME.fqn} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun createAndVerifyAboutEntity() {
        try {
            this.client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
            this.client.create(CreateEntity(TestConstants.TEST_ENTITY_NAME.fqn).column("id", "STRING"))
            val about = this.client.about(AboutEntity(TestConstants.TEST_ENTITY_NAME.fqn))
            assert(about.hasNext()) { "could not verify existence with about message" }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${TestConstants.TEST_ENTITY_NAME.fqn} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))

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
            client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
            client.create(CreateEntity(TestConstants.TEST_ENTITY_NAME.fqn).column("id", "STRING"))
            client.drop(DropEntity(TestConstants.TEST_ENTITY_NAME.fqn))
            val names = entityNames()
            assert(!names.contains(TestConstants.TEST_ENTITY_NAME.fqn)) { "Returned entity names do not contain ${TestConstants.TEST_ENTITY_NAME}." }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${TestConstants.TEST_ENTITY_NAME} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))

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
            client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
            client.create(CreateEntity(TestConstants.TEST_ENTITY_NAME.fqn).column("id", "STRING"))
            client.drop(DropEntity(TestConstants.TEST_ENTITY_NAME.fqn))
            val names = entityNames()
            assert(!names.contains(TestConstants.TEST_ENTITY_NAME.fqn)) { "Returned entity names do not contain ${TestConstants.TEST_ENTITY_NAME}." }
        } catch (e: StatusRuntimeException) {
            fail("Creating entity ${TestConstants.TEST_ENTITY_NAME} failed with status " + e.status)
        } finally {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))

            /* Make sure, that no transaction or locks are dangling. */
            val locks = this.client.locks()
            Assertions.assertFalse(locks.hasNext())
            locks.close()
        }
    }

    @Test
    fun aboutNonExistingEntity() {
        Assertions.assertThrows(StatusRuntimeException::class.java) {
            this.client.about(AboutEntity(TestConstants.TEST_ENTITY_NAME.fqn))
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
        client.list(ListEntities(TestConstants.TEST_SCHEMA.fqn)).forEach { t -> names.add(t.asString(DBO_CONSTANT)!!) }
        return names
    }
}
