package org.vitrivr.cottontail.dbms.entity.serialization

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.AbstractEntityTest
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files

/**
 * An abstract class for test cases that test for correctness of serialization
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
abstract class AbstractSerializationTest: AbstractEntityTest() {

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

    /** [Name.EntityName] of the test schema. */
    protected val entityName = this.schemaName.entity("serialization")

    /** [JDKRandomGenerator] used by this [AbstractSerializationTest]. */
    protected var random = JDKRandomGenerator(this.seed.toInt())

    /** The [ColumnDef]s used for the [AbstractSerializationTest]. */
    protected abstract val columns: Array<ColumnDef<*>>

    /** The printable name of this [AbstractSerializationTest]. */
    protected abstract val name: String

    /** [Name.EntityName] of the test schema. */
    override val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>>
        get() = listOf(this.entityName to this.columns.toList())

    /**
     * Executes the serialization test.
     */
    @RepeatedTest(3)
    fun test() {
        log("Starting serialization test on (${TestConstants.TEST_COLLECTION_SIZE} items).")
        /* Reset seed. */
        this.random.setSeed(this.seed)

        /* Start testing. */
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getCachedTxForDBO(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getCachedTxForDBO(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getCachedTxForDBO(entity) as EntityTx
            repeat(TestConstants.TEST_COLLECTION_SIZE) {
                val reference = this.nextRecord(it)
                val retrieved = entityTx.read((it + 1).toLong(), this.columns)
                for (i in 0 until retrieved.size) {
                    Assertions.assertTrue(reference[retrieved.columns[i]]!!.isEqual(retrieved[i]!!))
                }
            }
        } finally {
            txn.rollback()
        }
    }

    /**
     * Populates the test database with data.
     */
    override fun populateDatabase() {
        log("Inserting data (${TestConstants.TEST_COLLECTION_SIZE} items).")

        /* Reset seed. */
        this.random.setSeed(this.seed)

        /* Start inserting. */
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val catalogueTx = txn.getCachedTxForDBO(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getCachedTxForDBO(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getCachedTxForDBO(entity) as EntityTx

        /* Insert data and track how many entries have been stored for the test later. */
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            entityTx.insert(nextRecord(it))
        }
        txn.commit()
    }

    /**
     * Generates and returns the i-th [StandaloneRecord]. Usually generated randomly (which is up to the implementing class).
     *
     * @param i Index of the [StandaloneRecord]
     */
    abstract fun nextRecord(i: Int): StandaloneRecord
}