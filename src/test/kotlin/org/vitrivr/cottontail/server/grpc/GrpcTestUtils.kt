package org.vitrivr.cottontail.server.grpc

import org.apache.commons.lang3.RandomStringUtils
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.SimpleClient
import kotlin.random.Random


object GrpcTestUtils {

    const val TEST_ENTITY_FQN = "${TestConstants.TEST_SCHEMA}.${TestConstants.TEST_ENTITY}"
    const val TEST_ENTITY_FQN_WITH_WARREN = "warren.${TestConstants.TEST_SCHEMA}.${TestConstants.TEST_ENTITY}"
    const val TEST_VECTOR_ENTITY_FQN_INPUT = "${TestConstants.TEST_SCHEMA}.${TestConstants.TEST_VECTOR_ENTITY}"

    /** */
    const val STRING_COLUMN_NAME = "string_col"
    const val INT_COLUMN_NAME = "int_col"
    const val DOUBLE_COLUMN_NAME = "double_col"
    const val TWOD_COLUMN_NAME = "twod_col"
    const val TEST_ENTITY_TUPLE_COUNT = 1000L

    /**
     * Creates test schema.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestSchema(client: SimpleClient) {
        client.create(CreateSchema(TestConstants.TEST_SCHEMA))
    }

    /**
     * Drops test schema.
     *
     * @param client [SimpleClient] to use.
     */
    fun dropTestSchema(client: SimpleClient) {
        /* Teardown */
        try {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA))
        } catch (e: Exception) {
            //ignore
        }
    }

    /**
     * Creates test entity.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestEntity(client: SimpleClient) {
        val create = CreateEntity(TEST_ENTITY_FQN)
            .column(STRING_COLUMN_NAME, Type.STRING)
            .column(INT_COLUMN_NAME, Type.INTEGER)
            .column(DOUBLE_COLUMN_NAME, Type.DOUBLE)
        client.create(create)
    }

    /**
     * Creates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestVectorEntity(client: SimpleClient) {
        val create = CreateEntity(TEST_VECTOR_ENTITY_FQN_INPUT)
            .column(STRING_COLUMN_NAME, Type.STRING)
            .column(INT_COLUMN_NAME, Type.INTEGER)
            .column(TWOD_COLUMN_NAME, Type.FLOAT_VECTOR, 2)
        client.create(create)
    }

    /**
     * Populates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun populateTestEntity(client: SimpleClient) {
        val batch = BatchInsert().into(TEST_ENTITY_FQN)
            .columns(STRING_COLUMN_NAME, INT_COLUMN_NAME, DOUBLE_COLUMN_NAME)
        val random = Random.Default
        repeat(TEST_ENTITY_TUPLE_COUNT.toInt()) {
            batch.append(
                RandomStringUtils.randomAlphabetic(5),
                random.nextInt(0, 100),
                random.nextDouble(1.0)
            )
        }
        client.insert(batch)
    }

    /**
     * Populates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun populateVectorEntity(client: SimpleClient) {
        val batch = BatchInsert().into(TEST_VECTOR_ENTITY_FQN_INPUT)
            .columns(STRING_COLUMN_NAME, INT_COLUMN_NAME, TWOD_COLUMN_NAME)
        val random = Random.Default
        repeat(TEST_ENTITY_TUPLE_COUNT.toInt()) {
            val lat = random.nextFloat() + random.nextInt(0, 50)
            val lon = random.nextFloat() + random.nextInt(0, 50)
            val arr = arrayOf(lat, lon)
            batch.append(
                RandomStringUtils.randomAlphabetic(5),
                random.nextInt(0, 10),
                arr
            )
        }
        client.insert(batch)
    }
}