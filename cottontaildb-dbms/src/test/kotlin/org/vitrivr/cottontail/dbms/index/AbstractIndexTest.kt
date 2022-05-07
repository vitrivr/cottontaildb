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
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val ret = catalogueTx.createSchema(this.schemaName)
        txn.commit()
        return ret
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity() {
        log("Creating schema ${this.entityName}.")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        schemaTx.createEntity(this.entityName, *this.columns)
        txn.commit()
    }

    /**
     * Prepares and returns an empty test [Index].
     */
    protected fun prepareIndex() {
        log("Creating index ${this.indexName}.")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val config = this.indexType.descriptor.buildConfig(this.indexParams)
        entityTx.createIndex(this.indexName, this.indexType, listOf(this.indexColumn.name), config)
        txn.commit()
    }

    /**
     * Updates all indexes.
     */
    protected fun updateIndex() {
        log("Updating index ${this.indexName}.")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx
        indexTx.rebuild()
        txn.commit()
    }


    /**
     * Populates the test database with data.
     */
    protected fun populateDatabase() {
        log("Inserting data (${TestConstants.TEST_COLLECTION_SIZE} items).")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx

        /* Insert data and track how many entries have been stored for the test later. */
        for (i in 0..TestConstants.TEST_COLLECTION_SIZE) {
            entityTx.insert(nextRecord())
        }
        txn.commit()
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
        val catalogueTx = tx1.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = tx1.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = tx1.getTx(entity) as EntityTx
        val preCount = entityTx.count()
        entityTx.listIndexes().map {
            val indexTx = tx1.getTx(entityTx.indexForName(it)) as IndexTx
            indexTx.rebuild()
        }
        tx1.commit()

        /* Test count. */
        val tx2 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val countTx = tx2.getTx(entity) as EntityTx
        val postCount = countTx.count()
        if (postCount != preCount) {
            Assertions.fail<Unit>("Optimizing caused elements to disappear")
        }
        tx2.commit()
    }

    /**
     * Generates and returns a new [StandaloneRecord] for inserting into the database. Usually
     * generated randomly (which is up to the implementing class).
     */
    abstract fun nextRecord(): StandaloneRecord
}