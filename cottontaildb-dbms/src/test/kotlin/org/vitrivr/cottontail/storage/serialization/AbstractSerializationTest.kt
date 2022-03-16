package org.vitrivr.cottontail.storage.serialization

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.TransactionManager
import org.vitrivr.cottontail.dbms.execution.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files
import java.util.*

/**
 * An abstract class for test cases that test for correctness of [Value] serialization
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
abstract class AbstractSerializationTest {
    companion object {
        protected val LOGGER = LoggerFactory.getLogger(AbstractSerializationTest::class.java)
    }

    /** [Config] used for this [AbstractSerializationTest]. */
    private val config: Config = TestConstants.testConfig()

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(this.config.root)) {
            TxFileUtilities.delete(this.config.root)
        }
        Files.createDirectories(this.config.root)
    }

    /** Random seed used by this [AbstractSerializationTest]. */
    private val seed = System.currentTimeMillis()

    /** [Name.SchemaName] of the test schema. */
    private val schemaName = Name.SchemaName("test")

    /** [Name.EntityName] of the test schema. */
    protected val entityName = this.schemaName.entity("serialization")

    /** The [DefaultCatalogue] instance used for the [AbstractSerializationTest]. */
    private val catalogue: DefaultCatalogue = DefaultCatalogue(this.config)

    /** [JDKRandomGenerator] used by this [AbstractSerializationTest]. */
    protected var random = JDKRandomGenerator(this.seed.toInt())

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    protected val manager = TransactionManager(this.config.execution.transactionTableSize, this.catalogue.environment, this.config.execution.transactionHistorySize)

    /** The [ColumnDef]s used for the [AbstractSerializationTest]. */
    protected abstract val columns: Array<ColumnDef<*>>

    /** The printable name of this [AbstractSerializationTest]. */
    protected abstract val name: String

    /**
     * Initializes this [AbstractIndexTest] and prepares required [Entity] and [Index].
     */
    @BeforeEach
    fun initialize() {
        /* Prepare data structures. */
        prepareSchema()
        prepareEntity()

        /* Generate random data. */
        this.populateDatabase()

        /* Resets the random number generator. */
        this.reset()
    }

    /**
     * Closes the [DefaultCatalogue] and deletes all the files.
     */
    @AfterEach
    fun cleanup() {
        this.catalogue.close()
        TxFileUtilities.delete(this.config.root)
    }


    /**
     * Executes the serialization test.
     */
    @RepeatedTest(3)
    fun test() {
        log("Starting serialization test on (${TestConstants.collectionSize} items).")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx
            repeat(TestConstants.collectionSize) {
                val reference = this.nextRecord(it)
                val retrieved = entityTx.read(it.toLong(), this.columns)
                for (i in 0 until retrieved.size) {
                    Assertions.assertTrue(reference[retrieved.columns[i]]!!.isEqual(retrieved[i]!!))
                }
            }
        } finally {
            txn.rollback()
        }
    }


    /**
     * Resets the [SplittableRandom] for this [AbstractSerializationTest].
     */
    private fun reset() {
        this.random = JDKRandomGenerator(this.seed.toInt())
    }

    /**
     * Prepares and returns an empty test [Schema].
     */
    private fun prepareSchema(): Schema {
        log("Creating schema ${this.schemaName}.")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val ret = catalogueTx.createSchema(this.schemaName)
        txn.commit()
        return ret
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    private fun prepareEntity(): Entity {
        log("Creating schema ${this.entityName}.")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val ret = schemaTx.createEntity(this.entityName, *this.columns)
        txn.commit()
        return ret
    }

    /**
     * Populates the test database with data.
     */
    private fun populateDatabase() {
        log("Inserting data (${TestConstants.collectionSize} items).")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx

        /* Insert data and track how many entries have been stored for the test later. */
        repeat(TestConstants.collectionSize) {
            entityTx.insert(nextRecord(it))
        }
        txn.commit()
    }

    /**
     * Logs an information message regarding this [AbstractIndexTest].
     */
    private fun log(message: String) = LOGGER.info("${this.name}: $message")

    /**
     * Generates and returns the i-th [StandaloneRecord]. Usually generated randomly (which is up to the implementing class).
     *
     * @param i Index of the [StandaloneRecord]
     */
    abstract fun nextRecord(i: Int): StandaloneRecord
}