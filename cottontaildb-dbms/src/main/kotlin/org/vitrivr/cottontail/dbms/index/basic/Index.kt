package org.vitrivr.cottontail.dbms.index.basic

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.server.Instance

/**
 * Represents a (secondary) [Index] structure in the Cottontail DB data model. An [Index] belongs
 * to an [Entity] and can be used to index one to many [Column]s. Usually, [Entity]es allow for
 * faster data access.
 *
 * [Index] structures are uniquely identified by their [Name.IndexName].
 *
 * @see IndexTx
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface Index : DBO {

    /** [Entity] this [Index] belongs to. */
    override val parent: Entity

    /** The [Name.IndexName] of this [Index]. */
    override val name: Name.IndexName

    /** True, if the [Index] supports incremental updates, i.e., can be updated tuple by tuple. */
    val supportsIncrementalUpdate: Boolean

    /** True, if the [Index] backing this [IndexTx] supports asynchronous rebuilds. */
    val supportsAsyncRebuild: Boolean

    /** True, if the [Index] supports filtering an index-able range of the data. */
    val supportsPartitioning: Boolean

    /** The [IndexType] of this [Index]. */
    val type: IndexType

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param parent If parent [EntityTx] this [IndexTx] belongs to.
     */
    fun newTx(parent: EntityTx): IndexTx

    /**
     * Creates and returns a new [IndexRebuilder] for this [Index].
     *
     * @param context If the [QueryContext] that requested the [IndexRebuilder].
     * @return [IndexRebuilder]
     */
    fun newRebuilder(context: QueryContext): IndexRebuilder<*>

    /**
     * Creates and returns a new [AsyncIndexRebuilder] for this [Index].
     *
     * @param instance The [Instance] that requested the [AsyncIndexRebuilder].
     * @return [AsyncIndexRebuilder]
     */
    fun newAsyncRebuilder(instance: Instance): AsyncIndexRebuilder<*>
}