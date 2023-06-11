package org.vitrivr.cottontail.test

import org.apache.commons.lang3.RandomStringUtils
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.test.TestConstants.DOUBLE_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.ID_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.INT_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.STRING_COLUMN_NAME
import org.vitrivr.cottontail.test.TestConstants.TWOD_COLUMN_NAME
import kotlin.random.Random

/**
 *
 *
 * @author Silvan Heller & Ralph Gasser
 * @version 1.2.0
 */
object GrpcTestUtils {

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
            System.err.println("Failed to drop test schema ${TestConstants.TEST_SCHEMA} due to exception: ${e.message}.")
        }
    }

    /**
     * Creates test entity.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestEntity(client: SimpleClient) {
        val create = CreateEntity(TestConstants.TEST_ENTITY_NAME)
            .column(Name.ColumnName.parse(ID_COLUMN_NAME), Types.Long, autoIncrement = true)
            .column(Name.ColumnName.parse(STRING_COLUMN_NAME), Types.String)
            .column(Name.ColumnName.parse(INT_COLUMN_NAME), Types.Int)
            .column(Name.ColumnName.parse(DOUBLE_COLUMN_NAME), Types.Double)
        client.create(create)
    }

    /**
     * Creates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun createTestVectorEntity(client: SimpleClient) {
        val create = CreateEntity(TestConstants.TEST_VECTOR_ENTITY_NAME)
            .column(Name.ColumnName.parse(ID_COLUMN_NAME), Types.Long, autoIncrement = true)
            .column(Name.ColumnName.parse(STRING_COLUMN_NAME), Types.String)
            .column(Name.ColumnName.parse(INT_COLUMN_NAME), Types.Int)
            .column(Name.ColumnName.parse(TWOD_COLUMN_NAME), Types.FloatVector(2))
        client.create(create)
    }

    /**
     * Populates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun populateTestEntity(client: SimpleClient) {
        val batch = BatchInsert(TestConstants.TEST_ENTITY_NAME).columns(ID_COLUMN_NAME, STRING_COLUMN_NAME, INT_COLUMN_NAME, DOUBLE_COLUMN_NAME)
        val random = Random.Default
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            batch.any(
                null,
                RandomStringUtils.randomAlphabetic(5),
                random.nextInt(0, 100),
                random.nextDouble(1.0)
            )
        }
        client.insert(batch)
    }

    fun insertIntoTestEntity(client: SimpleClient, string: String = RandomStringUtils.randomAlphabetic(5), int: Int = Random.nextInt(0, 100), double: Double = Random.nextDouble(1.0)) {
        val insert = Insert(TestConstants.TEST_ENTITY_NAME).any(Pair(STRING_COLUMN_NAME, string), Pair(INT_COLUMN_NAME, int), Pair(DOUBLE_COLUMN_NAME, double))
        client.insert(insert)
    }

    /**
     * Creates a Lucene index on the [TestConstants.TEST_ENTITY_NAME].
     */
    fun createLuceneIndexOnTestEntity(client: SimpleClient) {
        val name = "lucene-index"
        client.create(CreateIndex(TestConstants.TEST_ENTITY_NAME, CottontailGrpc.IndexType.LUCENE).column(STRING_COLUMN_NAME).name(name))
        client.rebuild(RebuildIndex("${TestConstants.TEST_ENTITY_NAME.fqn}.$name"))
    }

    /**
     * Populates test entity for vector operations.
     *
     * @param client [SimpleClient] to use.
     */
    fun populateVectorEntity(client: SimpleClient) {
        val batch = BatchInsert(TestConstants.TEST_VECTOR_ENTITY_NAME.fqn)
            .columns(ID_COLUMN_NAME, STRING_COLUMN_NAME, INT_COLUMN_NAME, TestConstants.TWOD_COLUMN_NAME)
        val random = Random.Default
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            val lat = random.nextFloat() + random.nextInt(0, 50)
            val lon = random.nextFloat() + random.nextInt(0, 50)
            val arr = floatArrayOf(lat, lon)
            batch.any(
                null,
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