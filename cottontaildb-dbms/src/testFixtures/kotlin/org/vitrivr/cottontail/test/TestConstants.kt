package org.vitrivr.cottontail.test

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.config.ExecutionConfig
import org.vitrivr.cottontail.config.ServerConfig
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
    const val TEST_VECTOR_ENTITY = "test-vector-entity"

    val entityNames = listOf(TEST_ENTITY, TEST_VECTOR_ENTITY)

    val entityNamesProto = entityNames.map { GrpcTestUtils.toEn(it) }


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
