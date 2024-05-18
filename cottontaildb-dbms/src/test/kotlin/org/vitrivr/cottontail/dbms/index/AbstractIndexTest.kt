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
import org.vitrivr.cottontail.test.TestConstants

/**
 * An abstract class that tests [Index] structures in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.3.0
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
        val ctx = DefaultQueryContext("index-test-prepare-schema", this.instance, txn)
        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val ret = catalogueTx.createSchema(this.schemaName)
            txn.commit()
            return ret
        } catch (e: Throwable) {
            txn.abort()
            throw e
        }
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity() {
        log("Creating schema ${this.entityName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-prepare-entity", this.instance, txn)
        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(catalogueTx)
            schemaTx.createEntity(this.entityName, this.columns.map { it.name to ColumnMetadata(it.type, it.nullable, it.primary, it.autoIncrement, it.type.inline) })
            txn.commit()
        } catch (e: Throwable) {
            txn.abort()
            throw e
        }
    }

    /**
     * Prepares and returns an empty test [Index].
     */
    protected fun prepareIndex() {
        log("Creating index ${this.indexName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-prepare-index", this.instance, txn)
        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(catalogueTx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(schemaTx)
            val config = this.indexType.descriptor.buildConfig(this.indexParams)
            entityTx.createIndex(this.indexName, this.indexType, listOf(this.indexColumn.name), config)
            txn.commit()
        } catch (e: Throwable) {
            txn.abort()
            throw e
        }
    }

    /**
     * Updates all indexes.
     */
    protected fun updateIndex() {
        log("Updating index ${this.indexName}.")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-update-index", this.instance, txn)

        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(catalogueTx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(schemaTx)
            val index = entityTx.indexForName(this.indexName)
            val indexTx = index.newTx(entityTx)
            index.newRebuilder(ctx).rebuild(indexTx)
            txn.commit()
        } catch (e: Throwable) {
            txn.abort()
            throw e
        }
    }


    /**
     * Populates the test database with data.
     */
    protected fun populateDatabase() {
        log("Inserting data (${collectionSize} items).")
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test-populate", this.instance, txn)
        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(catalogueTx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(schemaTx)

            /* Insert data and track how many entries have been stored for the test later. */
            for (i in 0..this.collectionSize) {
                entityTx.insert(nextRecord())
            }
            txn.commit()
        } catch (e: Throwable) {
            txn.abort()
            throw e
        }

        /** Update database statistics. This is important for some applications. */
        this.instance.statistics.gatherStatisticsForEntity(this.entityName)
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
        val ctx1 = DefaultQueryContext("index-test-optimize", this.instance, txn1)
        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx1)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(catalogueTx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(schemaTx)

            val preCount = entityTx.count()
            entityTx.listIndexes().map {
                val index = entityTx.indexForName(it)
                val indexTx = index.newTx(entityTx)
                index.newRebuilder(ctx1).rebuild(indexTx)
            }
            txn1.commit()

            /* Test count. */
            val txn2 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
            val ctx2 = DefaultQueryContext("index-test-optimize-02", this.instance, txn2)
            try {
                val catalogueTx2 = this.catalogue.createOrResumeTx(ctx2)
                val schema2 = catalogueTx.schemaForName(this.schemaName)
                val schemaTx2 = schema2.newTx(catalogueTx2)
                val entity2 = schemaTx2.entityForName(this.entityName)
                val entityTx2 = entity2.createOrResumeTx(schemaTx2)
                val postCount = entityTx2.count()
                if (postCount != preCount) {
                    Assertions.fail<Unit>("Optimizing caused elements to disappear.")
                }
                txn2.commit()
            } catch (e: Throwable) {
                txn2.abort()
            }
        } catch (e: Throwable) {
            txn1.abort()
        }
    }

    /**
     * Generates and returns a new [StandaloneTuple] for inserting into the database. Usually
     * generated randomly (which is up to the implementing class).
     */
    abstract fun nextRecord(): StandaloneTuple
}