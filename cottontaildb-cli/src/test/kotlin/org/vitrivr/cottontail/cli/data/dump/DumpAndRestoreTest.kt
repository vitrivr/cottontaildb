package org.vitrivr.cottontail.cli.data.dump

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.fail
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.vitrivr.cottontail.client.language.ddl.DropEntity
import org.vitrivr.cottontail.data.Dumper
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.Restorer
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.test.TestConstants.TEST_ENTITY_NAME
import java.nio.file.Files
import kotlin.io.path.Path
import kotlin.io.path.deleteIfExists
import kotlin.time.ExperimentalTime

@ExperimentalTime
class DumpAndRestoreTest : AbstractClientTest() {

    @BeforeEach
    fun beforeEach() {
        this.startAndPopulateCottontail()
    }

    @AfterEach
    fun afterEach() {
        this.cleanup()
    }

    /**
     * Dumps all test entities in the different formats and restores them again.
     */
    @ParameterizedTest
    @EnumSource(value = Format::class)
    fun dumpAndRestore(format: Format) {
        /* Prepare path. */
        val path = Path("data", "test", "export", "test.dump")
        path.deleteIfExists()
        if (!Files.exists(path.parent)) {
            Files.createDirectories(path.parent)
        }

        /* Dump entity. */
        val dumps = Dumper.Zip(this.client, path, format, TestConstants.TEST_COLLECTION_SIZE/ 4).use {
            TestConstants.ALL_ENTITY_NAMES.map { entityName ->
                it.dump(entityName)
            }
        }

        /* Assert existence of database dump. */
        assert(Files.exists(path))
        assert(Files.size(path) > 1)

        /* Drop and restore entity. */
        Restorer.Zip(this.client, path, TestConstants.TEST_SCHEMA).use {r ->
            TestConstants.ALL_ENTITY_NAMES.forEachIndexed { index, entityName ->
                /* Number of entries and entries dumped must be equal. */
                Assertions.assertEquals(dumps[index], countElements(this.client, entityName))

                /* Drop and restore. */
                this.client.drop(DropEntity(entityName))
                val mf = r.manifest.entites.find { TestConstants.TEST_SCHEMA.entity(it.name) == entityName } ?: fail("Could not find manifest entry for $entityName.")
                r.restore(mf)

                /* Number of dumped and entries restored must be equal. */
                Assertions.assertEquals(dumps[index], countElements(this.client, entityName))
            }
        }

        /* Delete dump. */
        path.deleteIfExists()
    }

    /**
     *
     */
    @ParameterizedTest
    @EnumSource(value = Format::class)
    fun dumpNaNValues(format: Format) {
        GrpcTestUtils.insertIntoTestEntity(client, double = Double.NaN)
        GrpcTestUtils.insertIntoTestEntity(client)

        /* Determine path. */
        val path = Path("data", "test", "export", "${TEST_ENTITY_NAME.fqn}.dump")
        path.deleteIfExists()
        if (!Files.exists(path.parent)) {
            Files.createDirectories(path.parent)
        }

        /* Dump entity. */
        Dumper.Zip(this.client, path, format, TestConstants.TEST_COLLECTION_SIZE/ 4).use {
            it.dump(TEST_ENTITY_NAME)
        }

        val count = countElements(this.client, TEST_ENTITY_NAME)
        this.client.drop(DropEntity(TEST_ENTITY_NAME))

        /* Restore entity. */
        Restorer.Zip(this.client, path, TestConstants.TEST_SCHEMA).use { r ->
            val mf = r.manifest.entites.find { TestConstants.TEST_SCHEMA.entity(it.name) == TEST_ENTITY_NAME } ?: fail("Could not find manifest entry for $TEST_ENTITY_NAME.")
            r.restore(mf)
        }

        assert(count == countElements(this.client, TEST_ENTITY_NAME))

        /* Delete dump. */
        path.deleteIfExists()
    }
}