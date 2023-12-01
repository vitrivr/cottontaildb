package org.vitrivr.cottontail.dbms.index

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import org.vitrivr.cottontail.test.TestConstants

/**
 * An abstract class that tests [Index] structures in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class AbstractIndexTest: AbstractDatabaseTest() {

    /** [Name.EntityName] of the test schema. */
    protected val entityName = schemaName.entity("index")

    /** The [ColumnDef]s of the columns in the test [Entity]. */
    protected abstract val columns: Array<ColumnDef<*>>

    /** The [ColumnDef] of the  test [Index]. */
    protected abstract val indexColumn: ColumnDef<*>

    /** [Name.IndexName] of the test [Index]. */
    protected abstract val indexName: Name.IndexName

    /** [IndexType] of the test [Index]. */
    protected abstract val indexType: IndexType

    /** [IndexType] of the test [Index]. */
    protected open val indexParams: Map<String, String> = emptyMap()

    /** The [JDKRandomGenerator] random number generator. */
    protected val random = JDKRandomGenerator()

    /** */
    protected open val collectionSize: Int
        get() = TestConstants.TEST_COLLECTION_SIZE

    /**
     * Initializes this [AbstractIndexTest] and prepares required [Entity] and [Index].
     */
    @BeforeEach
    override fun initialize() {
        super.initialize()
        try {
            /* Prepare data structures. */
            prepareSchema()
            prepareEntity()
            prepareIndex()

            /* Add entries. */
            this.populateDatabase()

            /* Update the index. */
            this.updateIndex()
        } catch (e: Throwable) {
            this.log("Failed to prepare test due to exception: ${e.message}")
            throw e
        }
        log("Starting test...")
    }

    /**
     * Prepares and returns an empty test [Schema].
     */
    protected fun prepareSchema(): Schema {
        log("Creating schema ${this.schemaName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-prepare-schema", this.catalogue, txn)
        try {
            val catalogueTx = this.catalogue.newTx(ctx)
            val ret = catalogueTx.createSchema(this.schemaName)
            txn.commit()
            return ret
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity() {
        log("Creating schema ${this.entityName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-prepare-entity", this.catalogue, txn)
        try {
            val catalogueTx = this.catalogue.newTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            schemaTx.createEntity(this.entityName, this.columns.associate { it.name to ColumnMetadata(it.type, Compression.NONE, it.nullable, it.primary, it.autoIncrement) })
            txn.commit()
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }
    }

    /**
     * Prepares and returns an empty test [Index].
     */
    protected fun prepareIndex() {
        log("Creating index ${this.indexName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-prepare-index", this.catalogue, txn)
        try {
            val catalogueTx = this.catalogue.newTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.newTx(ctx)
            val config = this.indexType.descriptor.buildConfig(this.indexParams)
            entityTx.createIndex(this.indexName, this.indexType, listOf(this.indexColumn.name), config)
            txn.commit()
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }
    }

    /**
     * Updates all indexes.
     */
    protected fun updateIndex() {
        log("Updating index ${this.indexName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-update-index", this.catalogue, txn)

        try {
            val catalogueTx = this.catalogue.newTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.newTx(ctx)
            val index = entityTx.indexForName(this.indexName)
            index.newRebuilder(ctx).rebuild()
            txn.commit()
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }
    }


    /**
     * Populates the test database with data.
     */
    protected fun populateDatabase() {
        log("Inserting data (${collectionSize} items).")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-populate", this.catalogue, txn)
        try {
            val catalogueTx = this.catalogue.newTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.newTx(ctx)

            /* Insert data and track how many entries have been stored for the test later. */
            for (i in 0..this.collectionSize) {
                entityTx.insert(nextRecord())
            }
            txn.commit()
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }

        /** Update database statistics. This is important for some applications. */
        this.catalogue.statisticsManager.gatherStatisticsForEntity(this.entityName)
    }

    /**
     * Logs an information message regarding this [AbstractIndexTest].
     */
    fun log(message: String) = this.logger.info("Index test (${this.indexType}): $message")

    /**
     * Tests for correct optimization of index.
     */
    @Test
    fun optimizationCountTest() {
        log("Optimizing entity ${this.entityName}.")

        /* Create entry. */
        val txn1 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("index-test-optimize", this.catalogue, txn1)
        try {
            val catalogueTx = this.catalogue.newTx(ctx1)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx1)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.newTx(ctx1)
            val preCount = entityTx.count()
            entityTx.listIndexes().map {
                val rebuilder = entityTx.indexForName(it).newRebuilder(ctx1)
                rebuilder.rebuild()
            }
            txn1.commit()

            /* Test count. */
            val txn2 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
            val ctx2 = DefaultQueryContext("index-test-optimize-02", this.catalogue, txn2)
            try {
                val countTx = entity.newTx(ctx2)
                val postCount = countTx.count()
                if (postCount != preCount) {
                    Assertions.fail<Unit>("Optimizing caused elements to disappear")
                }
                txn2.commit()
            } catch (e: Throwable) {
                txn2.rollback()
            }
        } catch (e: Throwable) {
            txn1.rollback()
        }
    }

    /**
     * Generates and returns a new [StandaloneTuple] for inserting into the database. Usually
     * generated randomly (which is up to the implementing class).
     */
    abstract fun nextRecord(): StandaloneTuple
}