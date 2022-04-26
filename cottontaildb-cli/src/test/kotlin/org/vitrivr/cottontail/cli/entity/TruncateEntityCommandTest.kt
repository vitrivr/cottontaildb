package org.vitrivr.cottontail.cli.entity

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.GrpcTestUtils.TEST_ENTITY_FQN
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.GrpcTestUtils.toEn
import org.vitrivr.cottontail.test.TestConstants
import kotlin.time.ExperimentalTime

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

    @Test
    fun truncateEntity() {
        TestConstants.entityNamesProto.forEach { en ->
            TruncateEntityCommand.truncate(en, this.client, true)
            Assertions.assertEquals(0, countElements(this.client, en))
        }
    }

    @Test
    fun truncateEntityWithLucene() {
        GrpcTestUtils.createLuceneIndexOnTestEntity(this.client)
        TruncateEntityCommand.truncate(toEn(TEST_ENTITY_FQN), this.client, true)
        Assertions.assertEquals(0, countElements(this.client, TEST_ENTITY_FQN))
    }

}