package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.concurrent.withLock

/**
 * Represents an index in the Cottontail DB data model, that uses a persistent [HashMap] to map a
 * unique [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Ralph Gasser
 * @version 3.2.0
 */
class UQBTreeIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractBTreeIndex(name, parent) {

    companion object: IndexDescriptor<UQBTreeIndex> {
        /** True since [UQBTreeIndex] supports incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [UQBTreeIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = false

        /** False, since [UQBTreeIndex] does not support partitioning. */
        override val supportsPartitioning: Boolean = false

        /**
         * Opens a [UQBTreeIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [UQBTreeIndex].
         * @param entity The [Entity] that holds the [UQBTreeIndex].
         * @return The opened [LuceneIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): UQBTreeIndex = UQBTreeIndex(name, entity as DefaultEntity)

        /**
         * Generates and returns an empty [IndexConfig].
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<UQBTreeIndex> = UQBTreeIndexConfig

        /**
         * Returns the [UQBTreeIndexConfig]
         */
        override fun configBinding(): ComparableBinding = UQBTreeIndexConfig
    }

    /** The type of [DefaultIndex] */
    override val type: IndexType = IndexType.BTREE_UQ

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param parent The [EntityTx] that requested the [IndexTx].
     * @return [IndexTx]
     */
    override fun newTx(parent: EntityTx): IndexTx {
        require(parent is DefaultEntity.Tx) { "BTreeIndex can only be used with DefaultEntity.Tx" }
        return this.Tx(parent)
    }

    /**
     * Opens and returns a new [BTreeIndexRebuilder] object that can be used to rebuild with this [UQBTreeIndex].
     *
     * @param context [QueryContext] to open the [BTreeIndexRebuilder] for.
     * @return [BTreeIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext) = BTreeIndexRebuilder(this, context)

    /**
     * An [IndexTx] that affects this [UQBTreeIndex].
     */
    private inner class Tx(parent: DefaultEntity.Tx) : AbstractBTreeIndex.Tx(parent) {

        /** The Xodus [Store] used to store entries in the [UQBTreeIndex]. */
        override var store: Store = this.xodusTx.environment.openStore(this@UQBTreeIndex.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

        /**
         * Truncates (i.e., clears) the [UQBTreeIndex] backed by this [IndexTx].
         */
        override fun truncate() = this.txLatch.withLock {
            this.xodusTx.environment.truncateStore(this@UQBTreeIndex.name.storeName(), this.xodusTx)
            this.store = this.xodusTx.environment.openStore(this@UQBTreeIndex.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
        }
    }
}
