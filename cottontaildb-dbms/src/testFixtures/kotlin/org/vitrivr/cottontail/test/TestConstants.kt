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
 * @version 1.2.0
 */
object TestConstants {
    /** General size of collections used for testing. */
    const val TEST_COLLECTION_SIZE: Int = 50_000

    /** Maximum dimension used for vector generation. */
    const val smallVectorMaxDimension: Int = 128

    /** Maximum dimension used for vector generation. */
    const val mediumVectorMaxDimension: Int = 512

    /** Maximum dimension used for vector generation. */
    const val largeVectorMaxDimension: Int = 2048

    /** The [Name.SchemaName] used for gRPC tests */
    val TEST_SCHEMA = Name.SchemaName.create("test-schema")

    /** The [Name.EntityName] of the simple test entity used during gRPC tests. */
    val TEST_ENTITY_NAME = TEST_SCHEMA.entity("test-entity")

    /** The [Name.EntityName] of the vector test entity used during gRPC tests. */
    val TEST_VECTOR_ENTITY_NAME = TEST_SCHEMA.entity("test-vector-entity")

    /** The [Name.ColumnName] of the string test column. */
    val STRING_COLUMN_NAME = "string_col"

    /** The [Name.ColumnName] of the int test column. */
    val INT_COLUMN_NAME = "int_col"

    /** The [Name.ColumnName] of the double test column. */
    val DOUBLE_COLUMN_NAME = "double_col"

    /** The [Name.ColumnName] of the 2d-vector test column. */
    val TWOD_COLUMN_NAME = "twod_col"

    /** List of all [Name.EntityName] */
    val ALL_ENTITY_NAMES = listOf(TEST_ENTITY_NAME, TEST_VECTOR_ENTITY_NAME)

    /** Database Object name from the gRPC API */
    const val DBO_CONSTANT = "dbo"

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
