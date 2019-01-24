package ch.unibas.dmi.dbis.cottontail.database

import ch.unibas.dmi.dbis.cottontail.TestConstants
import ch.unibas.dmi.dbis.cottontail.database.schema.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.schema.Entity
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import org.junit.jupiter.api.AfterEach
import java.nio.file.Files

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach

class SchemaTest {

    private val schemaName = "schema-test"

    private var schema: Schema? = null


    @BeforeEach
    fun initialize() {
        schema = Schema.create(schemaName, TestConstants.config)
    }

    @AfterEach
    fun teardown() {
        schema?.drop()
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun SchemaCreateDropTest() {
        /* Check if directory exists. */
        assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}")))
        assertTrue(Files.isDirectory(TestConstants.config.root.resolve("schema_${schemaName}")))

        /* Check if catalogue file exists. */
        assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve(Schema.FILE_CATALOGUE)))
        assertFalse(Files.isDirectory(TestConstants.config.root.resolve("schema_${schemaName}").resolve(Schema.FILE_CATALOGUE)))

        /* Check if schema contains the expected number of entities (zero). */
        assertEquals(0, schema?.size)

        /* Drop schema. */
        schema?.drop()
        schema = null

        /* Check if directory exists. */
        assertFalse(Files.exists(TestConstants.config.root.resolve("schema_${schemaName}")))
        assertFalse(Files.exists(TestConstants.config.root.resolve("schema_${schemaName}").resolve(Schema.FILE_CATALOGUE)))
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun EntityCreateTest() {
        /* Create a few entities. */
        val entityNames = arrayOf("test1", "test2", "test3")
        for (name in entityNames) {
            schema?.createEntity(name, ColumnDef.withAttributes("id", "STRING"))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name")))
            assertTrue(Files.isDirectory(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name")))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name").resolve("col_id.db")))
            assertTrue(Files.isReadable(TestConstants.config.root.resolve("schema_${schemaName}").resolve("entity_$name").resolve(Entity.FILE_CATALOGUE)))
        }

        /* Check size of the schema. */
        assertEquals(entityNames.size, schema?.size)

        /* Check stored entity names. */
        entityNames.zip(schema!!.entities) { a, b ->
            assertEquals(a,b)
        }
    }
}