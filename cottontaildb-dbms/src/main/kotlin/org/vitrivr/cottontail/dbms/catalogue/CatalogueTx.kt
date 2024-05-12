package org.vitrivr.cottontail.dbms.catalogue

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * A [SubTransaction] that operates on a single [Catalogue]. [SubTransaction]s are a unit of isolation for data
 * operations (read/write).
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface CatalogueTx : SubTransaction {
    /** Reference to the [Catalogue] this [CatalogueTx] belongs to. */
    override val dbo: Catalogue

    /** The [QueryContext] this [SchemaTx] belongs to. Typically determined by parent [CatalogueTx]. */
    val context: QueryContext

    /** The [Transaction] this [EntityTx] belongs to. Typically determined by parent [CatalogueTx]. */
    override val transaction: Transaction
        get() = this.context.txn

    /**
     * Returns a list of [Name.SchemaName] held by this [Catalogue].
     *
     * @return [List] of all [Name.SchemaName].
     */
    fun listSchemas(): List<Name.SchemaName>

    /**
     * Returns the [Schema] for the given [Name.SchemaName].
     *
     * @param name [Name.SchemaName] of the [Schema].
     * @return [Schema]
     */
    fun schemaForName(name: Name.SchemaName): Schema

    /**
     * Creates a new, empty [Schema] with the given [Name.SchemaName]
     *
     * @param name The [Name.SchemaName] of the new [Schema].
     * @return Newly created [Schema] for use in context of current [SubTransaction]
     */
    fun createSchema(name: Name.SchemaName): Schema

    /**
     * Drops an existing [Schema] with the given [Name.SchemaName].
     *
     * @param name The [Name.SchemaName] of the [Schema] to be dropped.
     */
    fun dropSchema(name: Name.SchemaName)
}