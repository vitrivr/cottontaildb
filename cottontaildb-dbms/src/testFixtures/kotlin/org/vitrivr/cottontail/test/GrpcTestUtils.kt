package org.vitrivr.cottontail.test

import org.apache.commons.lang3.RandomStringUtils
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc
import kotlin.random.Random


object GrpcTestUtils {

    /**
     * Creates test schema.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestSchema(client: SimpleClient) {
        client.create(CreateSchema(TestConstants.TEST_SCHEMA.fqn))
    }

    /**
     * Drops test schema.
     *
     * @param client [SimpleClient] to use.
     */
    fun dropTestSchema(client: SimpleClient) {
        /* Teardown */
        try {
            client.drop(DropSchema(TestConstants.TEST_SCHEMA.fqn))
        } catch (e: Exception) {
            System.err.println("Failed to drop test schema ${TestConstants.TEST_SCHEMA} due to exception: ${e.message}.")
        }
    }

    /**
     * Creates test entity.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestEntity(client: SimpleClient) {
        val create = CreateEntity(TestConstants.TEST_ENTITY_NAME.fqn)
                .column(TestConstants.STRING_COLUMN_NAME, Type.STRING)
                .column(TestConstants.INT_COLUMN_NAME, Type.INTEGER)
                .column(TestConstants.DOUBLE_COLUMN_NAME, Type.DOUBLE)
        client.create(create)
    }

    /**
     * Creates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestVectorEntity(client: SimpleClient) {
        val create = CreateEntity(TestConstants.TEST_VECTOR_ENTITY_NAME.fqn)
                .column(TestConstants.STRING_COLUMN_NAME, Type.STRING)
                .column(TestConstants.INT_COLUMN_NAME, Type.INTEGER)
                .column(TestConstants.TWOD_COLUMN_NAME, Type.FLOAT_VECTOR, 2)
        client.create(create)
    }

    /**
     * Populates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun populateTestEntity(client: SimpleClient) {
        val batch = BatchInsert().into(TestConstants.TEST_ENTITY_NAME.fqn).columns(TestConstants.STRING_COLUMN_NAME, TestConstants.INT_COLUMN_NAME, TestConstants.DOUBLE_COLUMN_NAME)
        val random = Random.Default
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            batch.append(
                    RandomStringUtils.randomAlphabetic(5),
                    random.nextInt(0, 100),
                    random.nextDouble(1.0)
            )
        }
        client.insert(batch)
    }

    /**
     * Creates a Lucene index on the [TestConstants.TEST_ENTITY_NAME].
     */
    fun createLuceneIndexOnTestEntity(client: SimpleClient) {
        client.create(CreateIndex(TestConstants.TEST_ENTITY_NAME.fqn, TestConstants.STRING_COLUMN_NAME, CottontailGrpc.IndexType.LUCENE))
        client.optimize(OptimizeEntity(TestConstants.TEST_ENTITY_NAME.fqn))
    }

    /**
     * Populates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun populateVectorEntity(client: SimpleClient) {
        val batch = BatchInsert().into(TestConstants.TEST_VECTOR_ENTITY_NAME.fqn)
                .columns(TestConstants.STRING_COLUMN_NAME, TestConstants.INT_COLUMN_NAME, TestConstants.TWOD_COLUMN_NAME)
        val random = Random.Default
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            val lat = random.nextFloat() + random.nextInt(0, 50)
            val lon = random.nextFloat() + random.nextInt(0, 50)
            val arr = floatArrayOf(lat, lon)
            batch.append(
                    RandomStringUtils.randomAlphabetic(5),
                    random.nextInt(0, 10),
                    arr
            )
        }
        client.insert(batch)
    }

    fun countElements(client: SimpleClient, entityName: Name.EntityName): Long? {
        val query = Query(entityName.toString()).count()
        val res = client.query(query)
        return res.next().asLong(0)
    }
}