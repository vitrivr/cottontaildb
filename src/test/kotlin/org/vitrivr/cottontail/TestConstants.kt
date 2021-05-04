package org.vitrivr.cottontail

import org.vitrivr.cottontail.config.Config
import java.nio.file.Files
import java.util.*

/**
 * Some constants used during execution of unit tests.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object TestConstants {
    /** General size of collections used for testing. */
    const val collectionSize: Int = 50_000

    /** Maximum dimension used for vector generation. */
    const val smallVectorMaxDimension: Int = 128

    /** Maximum dimension used for vector generation. */
    const val mediumVectorMaxDimension: Int = 512

    /** Maximum dimension used for vector generation. */
    const val largeVectorMaxDimension: Int = 2048

    /** Schema used for grpc tests */
    const val TEST_SCHEMA = "test-schema"

    /** Entity used for grpc tests */
    const val TEST_ENTITY = "test-entity"

    /** Datbase Object name from the grpc api */
    const val DBO_CONSTANT = "dbo"

    /**
     * Creates a new test configuration.
     *
     * @return [Config]
     */
    fun testConfig() = Config(
        root = Files.createTempDirectory("cottontaildb-test-${UUID.randomUUID()}"),
        cli = false
    )
}
