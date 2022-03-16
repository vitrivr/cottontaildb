package org.vitrivr.cottontail.dbms.catalogue

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.TransactionType
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * A set of unit tests to test basic [Schema] functionality.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class SchemaTest: AbstractDatabaseTest() {

    /** List of [DefaultEntity] to create. */
    private val entityNames = arrayOf(
        this.schemaName.entity("one"),
        this.schemaName.entity("two"),
        this.schemaName.entity("three")
    )

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithCommitTest() {
        /* Create a few entities. */
        val entityNames = arrayOf(this.schemaName.entity("one"), this.schemaName.entity("two"), this.schemaName.entity("three"))

        /* Transaction 1: Create entity. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM)

        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.createSchema(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Types.String))
            }
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in entityNames) {
                Assertions.assertDoesNotThrow {
                    schemaTx2.entityForName(name)
                }
            }

            /* Check size and content of schema. */
            val fetchedEntities = schemaTx2.listEntities()
            assertEquals(entityNames.size, fetchedEntities.size)
            assertTrue(fetchedEntities.all { entityNames.contains(it) })
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
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.createSchema(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Types.String))
            }

            /* Drop newly created entity. */
            schemaTx1.dropEntity(entityNames[1])

            /* Create new entity with the same name. */
            schemaTx1.createEntity(
                entityNames[1],
                ColumnDef(entityNames[1].column("id1"), Types.Long),
                ColumnDef(entityNames[1].column("id2"), Types.Int)
            )
            txn1.commit()
        } catch (t: Throwable) {
            txn1.rollback()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in entityNames) {
                Assertions.assertDoesNotThrow {
                    schemaTx2.entityForName(name)
                }
            }

            /* Check size and content of schema. */
            val fetchedEntities = schemaTx2.listEntities()
            assertEquals(entityNames.size, fetchedEntities.size)
            assertTrue(fetchedEntities.all { entityNames.contains(it) })
        } finally {
            txn2.rollback()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithRollbackTest() {
        /* Transaction 0: Create schema (as preparation). */
        val txn0 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx0 = txn0.getTx(this.catalogue) as CatalogueTx
            catalogueTx0.createSchema(this.schemaName)
        } finally {
            txn0.commit()
        }

        /* Transaction 1: Create entity. */
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.schemaForName(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Types.String))
            }
        } finally {
            txn1.rollback()
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM)
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
        val txn1 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx1 = txn1.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx1.createSchema(this.schemaName)
            val schemaTx1 = txn1.getTx(schema) as SchemaTx
            for (name in this.entityNames) {
                schemaTx1.createEntity(name, ColumnDef(name.column("id"), Types.String))
            }
        } finally {
            txn1.commit()
        }

        /* Transaction 2: Truncate. */
        val txn2 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx2 = txn2.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = txn2.getTx(schema) as SchemaTx
            for (name in this.entityNames) {
                val entity = schemaTx2.entityForName(name)
                assertEquals(1, entity.numberOfColumns)
                schemaTx2.dropEntity(name)
                schemaTx2.createEntity(
                        name,
                        ColumnDef(name.column("id"), Types.String),
                        ColumnDef(name.column("value"), Types.String)
                )
            }
        } finally {
            txn2.commit()
        }

        /* Transaction 2: Truncate. */
        val txn3 = this.manager.TransactionImpl(TransactionType.SYSTEM)
        try {
            val catalogueTx3 = txn3.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx3.schemaForName(this.schemaName)
            val schemaTx3 = txn3.getTx(schema) as SchemaTx
            for (name in this.entityNames) {
                val entity = schemaTx3.entityForName(name)
                assertEquals(2, entity.numberOfColumns)
            }
        } finally {
            txn3.commit()
        }
    }
}
