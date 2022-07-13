package org.vitrivr.cottontail.dbms.index

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            schemaTx.createEntity(this.entityName, *this.columns)
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx
            val index = entityTx.indexForName(this.indexName)
            index.newRebuilder(txn).rebuild()
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx

            /* Insert data and track how many entries have been stored for the test later. */
            for (i in 0..this.collectionSize) {
                entityTx.insert(nextRecord())
            }
            txn.commit()
        } catch (e: Throwable) {
            txn.rollback()
            throw e
        }
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
        val tx1 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = tx1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = tx1.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = tx1.getTx(entity) as EntityTx
            val preCount = entityTx.count()
            entityTx.listIndexes().map {
                val rebuilder = entityTx.indexForName(it).newRebuilder(tx1)
                rebuilder.rebuild()
            }
            tx1.commit()

            /* Test count. */
            val tx2 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
            try {
                val countTx = tx2.getTx(entity) as EntityTx
                val postCount = countTx.count()
                if (postCount != preCount) {
                    Assertions.fail<Unit>("Optimizing caused elements to disappear")
                }
                tx2.commit()
            } catch (e: Throwable) {
                tx2.rollback()
            }
        } catch (e: Throwable) {
            tx1.rollback()
        }
    }

    /**
     * Generates and returns a new [StandaloneRecord] for inserting into the database. Usually
     * generated randomly (which is up to the implementing class).
     */
    abstract fun nextRecord(): StandaloneRecord
}