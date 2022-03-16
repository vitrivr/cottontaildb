package org.vitrivr.cottontail.dbms

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.TransactionType
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

abstract class AbstractEntityTest: AbstractDatabaseTest() {

    /** [Name.EntityName] of the test schema. */
    protected abstract val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>>

    /**
     * Initializes this [AbstractDatabaseTest] and prepares required [Entity] and [Index].
     */
    override fun initialize() {
        super.initialize() /* Call super. */

        /* Update the index. */
        this.logger.info("Preparing database...")

        /* Prepare data structures. */
        prepareSchema()
        prepareEntity()

        /* Populate database with data. */
        this.populateDatabase()

        /* Update the index. */
        this.logger.info("Starting test...")
    }

    /**
     * Prepares and returns an empty test [Schema].
     */
    protected fun prepareSchema(): Schema {
        this.logger.info("Creating schema ${this.schemaName}.")
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val ret = catalogueTx.createSchema(this.schemaName)
        txn.commit()
        return ret
    }

    /**
     * Prepares and returns an empty test [Entity].
     */
    protected fun prepareEntity() {
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        for (e in this.entities) {
            this.logger.info("Creating schema ${e.first}.")
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            schemaTx.createEntity(e.first, *e.second.map { it }.toTypedArray())
        }
        txn.commit()
    }

    /**
     * Populates database with test data.
     */
    protected abstract fun populateDatabase()
}