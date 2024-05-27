package org.vitrivr.cottontail.test

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.config.ServerConfig
import org.vitrivr.cottontail.core.database.Name

import java.nio.file.Files
import java.util.*

/**
 * Some constants used during execution of unit tests.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
object TestConstants {
    /** General size of collections used for testing. */
    const val TEST_COLLECTION_SIZE: Int = 50_000

    /** Collection size of the SIFT test collection. */
    const val SIFT_TEST_COLLECTION_SIZE: Int = 9_999

    /** Maximum dimension used for vector generation. */
    const val SMALL_VECTOR_MAX_DIMENSION: Int = 128

    /** Maximum dimension used for vector generation. */
    const val MEDIUM_VECTOR_MAX_DIMENSION: Int = 512

    /** Maximum dimension used for vector generation. */
    const val LARGE_VECTOR_MAX_DIMENSION: Int = 2048


    /** The [Name.ColumnName] of the id test column. */
    const val ID_COLUMN_NAME = "id_col"

    /** The [Name.ColumnName] of the string test column. */
    const val STRING_COLUMN_NAME = "string_col"

    /** The [Name.ColumnName] of the int test column. */
    const val INT_COLUMN_NAME = "int_col"

    /** The [Name.ColumnName] of the double test column. */
    const val DOUBLE_COLUMN_NAME = "double_col"

    /** The [Name.ColumnName] of the 2d-vector test column. */
    const val TWOD_COLUMN_NAME = "twod_col"

    /** Database Object name from the gRPC API */
    const val DBO_CONSTANT = "dbo"

    /** The [Name.SchemaName] used for gRPC tests */
    val TEST_SCHEMA = Name.SchemaName.create("test-schema")

    /** The [Name.EntityName] of the simple test entity used during gRPC tests. */
    val TEST_ENTITY_NAME = TEST_SCHEMA.entity("test-entity")

    /** The [Name.EntityName] of the vector test entity used during gRPC tests. */
    val TEST_VECTOR_ENTITY_NAME = TEST_SCHEMA.entity("test-vector-entity")

    /** List of all [Name.EntityName] */
    val ALL_ENTITY_NAMES = listOf(TEST_ENTITY_NAME, TEST_VECTOR_ENTITY_NAME)

    /**
     * Creates a new test configuration.
     *
     * @return [Config]
     */
    fun testConfig() = Config(
            root = Files.createTempDirectory("cottontaildb-test-${UUID.randomUUID()}"),
            execution = ExecutionConfig(coreThreads = 2, maxThreads = 4),
            server = ServerConfig(connectionThreads = 2)
    )
}
