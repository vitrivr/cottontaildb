package org.vitrivr.cottontail.dbms.catalogue

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files

/**
 * A set of unit tests to test basic [DefaultCatalogue] functionality.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class CatalogueTest: AbstractDatabaseTest() {

    /** [Config] used for this [CatalogueTest]. */
    private val config: Config = TestConstants.testConfig()

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(this.config.root)) {
            TxFileUtilities.delete(this.config.root)
        }
        Files.createDirectories(this.config.root)
    }

    /**
     * Creates a new [DefaultSchema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createSchemaCommitTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-schema-01", this.catalogue, txn1)
        try {
            val catalogueTxn1 = this.catalogue.newTx(ctx1)
            catalogueTxn1.createSchema(this.schemaName)
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Read and compare schema. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("create-schema-02", this.catalogue, txn2)
        try {
            val catalogueTxn2 = this.catalogue.newTx(ctx2)
            val schema = catalogueTxn2.schemaForName(this.schemaName)
            val schemaTxn2 = schema.newTx(ctx2)

            /* Check if schema contains the expected number of entities (zero). */
            Assertions.assertEquals(0, schemaTxn2.listEntities().size)
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createSchemaRollbackTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-schema-01", this.catalogue, txn1)
        try {
            val catalogueTxn1 = this.catalogue.newTx(ctx1)
            catalogueTxn1.createSchema(this.schemaName)
        } finally {
            txn1.rollback()
        }

        /* Transaction 2: Read and compare schema. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("create-schema-02", this.catalogue, txn2)
        try {
            val catalogueTxn2 = this.catalogue.newTx(ctx2)

            /* Read schema (should throw error). */
            Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) {
                catalogueTxn2.schemaForName(this.schemaName)
            }
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and then drops it.
     *
     * Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun dropSchemaCommitTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("drop-schema-01", this.catalogue, txn1)
        try {
            val catalogueTxn1 = this.catalogue.newTx(ctx1)
            catalogueTxn1.createSchema(this.schemaName)
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }


        /* Transaction 2: Read, check and drop schema. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("drop-schema-02", this.catalogue, txn2)
        try {
            val catalogueTxn2 = this.catalogue.newTx(ctx2)
            catalogueTxn2.schemaForName(this.schemaName)

            /* Drop schema. */
            catalogueTxn2.dropSchema(this.schemaName)
            txn2.commit()
        } catch (t: Throwable) {
            txn2.rollback()
            throw t
        }

        /* Transaction 3: Read and compare schema. */
        val txn3 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx3 = DefaultQueryContext("drop-schema-03", this.catalogue, txn3)

        try {
            val catalogueTxn3 = this.catalogue.newTx(ctx3)

            /* Read schema (should throw error). */
            Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) {
                catalogueTxn3.schemaForName(this.schemaName)
            }
        } finally {
            txn3.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and then drops it followed by a rollback.
     *
     * Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun dropSchemaRollbackTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("drop-schema-01", this.catalogue, txn1)
        try {
            val catalogueTxn1 = this.catalogue.newTx(ctx1)
            catalogueTxn1.createSchema(this.schemaName)
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }


        /* Transaction 2: Read, check and drop schema. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("drop-schema-02", this.catalogue, txn2)
        try {
            val catalogueTxn2 = this.catalogue.newTx(ctx2)
            Assertions.assertDoesNotThrow {
                catalogueTxn2.schemaForName(this.schemaName)
            }

            /* Drop schema. */
            catalogueTxn2.dropSchema(this.schemaName)
        } finally {
            txn2.rollback()
        }


        /* Transaction 3: Read and compare schema. */
        val txn3 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx3 = DefaultQueryContext("drop-schema-03", this.catalogue, txn3)
        try {
            val catalogueTxn3 = this.catalogue.newTx(ctx3)
            Assertions.assertDoesNotThrow {
                catalogueTxn3.schemaForName(this.schemaName)
            }
        } finally {
            txn3.rollback()
        }
    }

    /**
     * Creates a new [DefaultSchema] and then drops it.
     *
     * Runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createAndDropSchemaSingleTransactionTest() {
        /* Transaction 1: Create schema. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-drop-schema-01", this.catalogue, txn1)
        try {
            val catalogueTxn1 = this.catalogue.newTx(ctx1)
            Assertions.assertDoesNotThrow {
                catalogueTxn1.createSchema(this.schemaName)
                catalogueTxn1.dropSchema(this.schemaName)
            }
        } finally {
            txn1.commit()
        }

        /* Transaction 2: Read and compare schema. */
        val txn3 = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx3 = DefaultQueryContext("create-drop-schema-01", this.catalogue, txn3)
        try {
            val catalogueTxn3 = this.catalogue.newTx(ctx3)

            /* Read schema (should throw error). */
            Assertions.assertThrows(DatabaseException.SchemaDoesNotExistException::class.java) {
                catalogueTxn3.schemaForName(this.schemaName)
            }
        } finally {
            txn3.rollback()
        }
    }
}
