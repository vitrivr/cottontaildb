package org.vitrivr.cottontail.dbms.catalogue

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * A set of unit tests to test basic [Schema] functionality.
 *
 * @author Ralph Gasser
 * @version 1.4.0
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
        val txn1 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-entity-test-01", this.instance, txn1)

        try {
            val catalogueTx1 = this.catalogue.createOrResumeTx(ctx1)
            val schema = catalogueTx1.createSchema(this.schemaName)
            val schemaTx1 = schema.newTx(catalogueTx1)
            for (name in entityNames) {
                schemaTx1.createEntity(name, listOf(name.column("id") to ColumnMetadata(Types.String, nullable = false, primary = true, autoIncrement = false)))
            }
            txn1.commit()
        } catch (t: Throwable) {
            txn1.abort()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("create-entity-test-02", this.instance, txn2)
        try {
            val catalogueTx2 = this.catalogue.createOrResumeTx(ctx2)
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = schema.newTx(catalogueTx2)
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
            txn2.abort()
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
        val txn1 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-entity-test-01", this.instance, txn1)
        try {
            val catalogueTx1 = this.catalogue.createOrResumeTx(ctx1)
            val schema = catalogueTx1.createSchema(this.schemaName)
            val schemaTx1 = schema.newTx(catalogueTx1)
            for (name in entityNames) {
                schemaTx1.createEntity(name, listOf(name.column("id") to ColumnMetadata(Types.String, nullable = false, primary = true, autoIncrement = false)))
            }

            /* Drop newly created entity. */
            schemaTx1.dropEntity(entityNames[1])

            /* Create new entity with the same name. */
            schemaTx1.createEntity(
                entityNames[1],
                listOf(
                    entityNames[1].column("id1") to ColumnMetadata(Types.Long, nullable = false, primary = true, autoIncrement = false),
                    entityNames[1].column("id2") to ColumnMetadata(Types.Int, nullable = false, primary = false, autoIncrement = false)
                )
            )
            txn1.commit()
        } catch (t: Throwable) {
            txn1.abort()
            throw t
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("create-entity-test-02", this.instance, txn2)
        try {
            val catalogueTx2 = this.catalogue.createOrResumeTx(ctx2)
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = schema.newTx(catalogueTx2)
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
            txn2.abort()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun createEntityWithRollbackTest() {
        /* Transaction 0: Create schema (as preparation). */
        val txn0 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx0 = DefaultQueryContext("create-entity-test-01", this.instance, txn0)
        try {
            val catalogueTx0 = this.catalogue.createOrResumeTx(ctx0)
            catalogueTx0.createSchema(this.schemaName)
        } finally {
            txn0.commit()
        }

        /* Transaction 1: Create entity. */
        val txn1 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-entity-test-02", this.instance, txn1)
        try {
            val catalogueTx1 = this.catalogue.createOrResumeTx(ctx1)
            val schema = catalogueTx1.schemaForName(this.schemaName)
            val schemaTx1 = schema.newTx(catalogueTx1)
            for (name in entityNames) {
                schemaTx1.createEntity(name, listOf(name.column("id") to ColumnMetadata(Types.String, nullable = false, primary = true, autoIncrement = false)))
            }
        } finally {
            txn1.abort()
        }

        /* Transaction 2: Query. */
        val txn2 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("create-entity-test-03", this.instance, txn2)
        try {
            val catalogueTx2 = this.catalogue.createOrResumeTx(ctx2)
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = schema.newTx(catalogueTx2)
            for (name in entityNames) {
                Assertions.assertThrows(DatabaseException.EntityDoesNotExistException::class.java) {
                    schemaTx2.entityForName(name)
                }
            }
        } finally {
            txn2.abort()
        }
    }

    /**
     * Creates a new [Schema] and runs some basic tests on the existence of the required files and initialization of the correct attributes.
     */
    @Test
    fun replaceEntityWithCommitTest() {
        /* Transaction 1: Create entity. */
        val txn1 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("create-entity-test-01", this.instance, txn1)
        try {
            val catalogueTx1 = this.catalogue.createOrResumeTx(ctx1)
            val schema = catalogueTx1.createSchema(this.schemaName)
            val schemaTx1 = schema.newTx(catalogueTx1)
            for (name in this.entityNames) {
                schemaTx1.createEntity(name, listOf(name.column("id") to ColumnMetadata(Types.String, false, primary = true, autoIncrement = false)))
            }
        } finally {
            txn1.commit()
        }

        /* Transaction 2: Truncate. */
        val txn2 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx2 = DefaultQueryContext("create-entity-test-02", this.instance, txn2)
        try {
            val catalogueTx2 = this.catalogue.createOrResumeTx(ctx2)
            val schema = catalogueTx2.schemaForName(this.schemaName)
            val schemaTx2 = schema.newTx(catalogueTx2)
            for (name in this.entityNames) {
                val entity = schemaTx2.entityForName(name)
                val entityTx = entity.createOrResumeTx(schemaTx2)
                assertEquals(1, entityTx.listColumns().size)
                schemaTx2.dropEntity(name)
                schemaTx2.createEntity(
                    name,
                    listOf(
                        name.column("id") to ColumnMetadata(Types.String, nullable = false, primary = true, autoIncrement = false),
                        name.column("value") to ColumnMetadata(Types.String, nullable = false, primary = false, autoIncrement = false)
                    )
                )
            }
        } finally {
            txn2.commit()
        }

        /* Transaction 2: Truncate. */
        val txn3 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx3 = DefaultQueryContext("create-entity-test-02", this.instance, txn3)
        try {
            val catalogueTx3 = this.catalogue.createOrResumeTx(ctx3)
            val schema = catalogueTx3.schemaForName(this.schemaName)
            val schemaTx3 = schema.newTx(catalogueTx3)
            for (name in this.entityNames) {
                val entity = schemaTx3.entityForName(name)
                val entityTx3 = entity.createOrResumeTx(schemaTx3)
                assertEquals(2, entityTx3.listColumns().size)
            }
        } finally {
            txn3.commit()
        }
    }
}
