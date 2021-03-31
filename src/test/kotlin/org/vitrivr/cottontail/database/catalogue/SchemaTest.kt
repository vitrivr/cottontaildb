package org.vitrivr.cottontail.database.catalogue

import org.junit.Assert
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

/**
 * A set of unit tests to test basic [Schema] functionality.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SchemaTest {
    /** [Name.SchemaName] of the test schema. */
    private val schemaName = Name.SchemaName("test")

    /** List of [DefaultEntity] to create. */
    private val entityNames = arrayOf(
        this.schemaName.entity("one"),
        this.schemaName.entity("two"),
        this.schemaName.entity("three")
    )

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(TestConstants.config.root)) {
            TxFileUtilities.delete(TestConstants.config.root)
        }
        Files.createDirectories(TestConstants.config.root)
    }

    /** The [TransactionManager] used for this [CatalogueTest] instance. */
    private val manager = TransactionManager(
        Executors.newFixedThreadPool(1) as ThreadPoolExecutor,
        TestConstants.config.execution.transactionTableSize,
        TestConstants.config.execution.transactionHistorySize
    )

    /** The [DefaultCatalogue] object to run the test with. */
    private val catalogue: DefaultCatalogue = DefaultCatalogue(TestConstants.config)

    @BeforeEach
    fun initialize() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            catalogueTx.createSchema(this.schemaName)
        } finally {
            txn.commit()
        }
    }


    @AfterEach
    fun teardown() {
        this.catalogue.close()
        TxFileUtilities.delete(TestConstants.config.root)
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithCommitTest() {
        /* Create a few entities. */
        val entityNames = arrayOf(this.schemaName.entity("one"), this.schemaName.entity("two"), this.schemaName.entity("three"))

        /* Transaction 1: Create entity. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)

        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.schemaForName(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB)
            }
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in entityNames) {
                val entity = schemaTx2.entityForName(name)
                assertTrue(Files.isReadable(entity.path))
                assertTrue(Files.isDirectory(entity.path))
            }

            /* Check size and content of schema. */
            val fetchedEntities = schemaTx2.listEntities()
            assertEquals(entityNames.size, fetchedEntities.size)
            assertTrue(fetchedEntities.all { entityNames.contains(it.name) })
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createAndDropEntityWithCommitTest() {
        /* Create a few entities. */
        val entityNames = arrayOf(this.schemaName.entity("one"), this.schemaName.entity("two"), this.schemaName.entity("three"))

        /* Transaction 1: Create entity. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.schemaForName(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB)
            }

            /* Drop newly created entity. */
            schemaTx1.dropEntity(entityNames[1])

            /* Create new entity with the same name. */
            schemaTx1.createEntity(entityNames[1],
                    ColumnDef(entityNames[1].column("id1"), Type.Long) to ColumnEngine.MAPDB,
                    ColumnDef(entityNames[1].column("id2"), Type.Int) to ColumnEngine.MAPDB
            )
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in entityNames) {
                val entity = schemaTx2.entityForName(name)
                assertTrue(Files.isReadable(entity.path))
                assertTrue(Files.isDirectory(entity.path))
            }

            /* Check size and content of schema. */
            val fetchedEntities = schemaTx2.listEntities()
            assertEquals(entityNames.size, fetchedEntities.size)
            assertTrue(fetchedEntities.all { entityNames.contains(it.name) })
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithRollbackTest() {
        /* Transaction 1: Create entity. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.schemaForName(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB)
            }
        } finally {
            txn1.rollback()
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in entityNames) {
                Assertions.assertThrows(DatabaseException.EntityDoesNotExistException::class.java) {
                    schemaTx2.entityForName(name)
                }
            }
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun replaceEntityWithCommitTest() {
        /* Transaction 1: Create entity. */
        val txn1 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.schemaForName(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in this.entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB)
            }
        } finally {
            txn1.commit()
        }

        /* Transaction 2: Truncate. */
        val txn2 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in this.entityNames) {
                val entity = schemaTx2.entityForName(name)
                Assert.assertEquals(1, entity.numberOfColumns)
                schemaTx2.dropEntity(name)
                schemaTx2.createEntity(
                        name,
                        ColumnDef(name.column("id"), Type.String) to ColumnEngine.MAPDB,
                        ColumnDef(name.column("value"), Type.String) to ColumnEngine.MAPDB
                )
            }
        } finally {
            txn2.commit()
        }

        /* Transaction 2: Truncate. */
        val txn3 = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val catalogueTx3 = txn3.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx3.schemaForName(this.schemaName)
            val schemaTx3 = txn3.getTx(schema) as SchemaTx
            for (name in this.entityNames) {
                val entity = schemaTx3.entityForName(name)
                Assert.assertEquals(2, entity.numberOfColumns)
            }
        } finally {
            txn3.commit()
        }
    }
}
