package org.vitrivr.cottontail.cli.entity

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.TestConstants
import kotlin.time.ExperimentalTime

/**
 * A series of test cases for the [TruncateEntityCommand].
 *
 * @version 1.0.0
 * @author Silvan Heller
 */
@ExperimentalTime
class TruncateEntityCommandTest : AbstractClientTest() {

    @BeforeEach
    fun beforeEach() {
        this.startAndPopulateCottontail()
    }

    @AfterEach
    fun afterEach() {
        this.cleanup()
    }

    /**
     * Truncates an entity and checks the count before and afterwards.
     */
    @Test
    fun truncateEntity() {
        TestConstants.ALL_ENTITY_NAMES.forEach { en ->
            TruncateEntityCommand.truncate(en, this.client, true)
            Assertions.assertEquals(0L, countElements(this.client, en))
        }
    }

    /**
     * Tries to truncate an entity that has a Lucene index.
     */
    @Test
    fun truncateEntityWithLucene() {
        GrpcTestUtils.createLuceneIndexOnTestEntity(this.client)
        TruncateEntityCommand.truncate(TestConstants.TEST_ENTITY_NAME, this.client, true)
        Assertions.assertEquals(0, countElements(this.client, TestConstants.TEST_ENTITY_NAME))
    }
}