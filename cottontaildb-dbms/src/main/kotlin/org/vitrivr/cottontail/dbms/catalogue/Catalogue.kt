package org.vitrivr.cottontail.dbms.catalogue

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * The main catalogue in Cottontail DB. It contains references to all the [Schema]s managed by
 * Cottontail DB and is the main way of accessing these [Schema]s and creating new ones.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Catalogue : DBO {
    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    val functions: FunctionRegistry

    /** Constant name of the [Catalogue] object. */
    override val name: Name.RootName

    /** Constant parent [DBO], which is null in case of the [Catalogue]. */
    override val parent: DBO?

    /**
     * Creates and returns a new [CatalogueTx] for the given [QueryContext].
     *
     * @param transaction The [QueryContext] to create the [CatalogueTx] for.
     * @return New [CatalogueTx]
     */
    fun newTx(transaction: Transaction): CatalogueTx
}