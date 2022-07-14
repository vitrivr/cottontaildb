package org.vitrivr.cottontail.dbms.index.ivfpq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class IVFPQIndex(name: Name.IndexName, parent: DefaultEntity): AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [PQIndex].
     */
    companion object: IndexDescriptor<IVFPQIndex> {
        /** [Logger] instance used by [IVFPQIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(IVFPQIndex::class.java)

        /** False since [PQIndex] currently doesn't support incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [PQIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = true

        /** True since [PQIndex] supports partitioning. */
        override val supportsPartitioning: Boolean = true

        /**
         * Opens a [PQIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [Entity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): IVFPQIndex = IVFPQIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [IVFPQIndex].
         *
         * @param name The [Name.IndexName] of the [IVFPQIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val store = (catalogue as DefaultCatalogue).environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize IVFPQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [IVFPQIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            (catalogue as DefaultCatalogue).environment.removeStore(name.storeName(), context.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize IVFPQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns a [IVFPQIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [IVFPQIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<IVFPQIndex> = IVFPQIndexConfig(
            parameters[IVFPQIndexConfig.KEY_DISTANCE]?.let { Name.FunctionName(it) } ?: EuclideanDistance.FUNCTION_NAME,
            parameters[IVFPQIndexConfig.KEY_NUM_CENTROIDS]?.toInt() ?: IVFPQIndexConfig.DEFAULT_CENTROIDS,
            parameters[IVFPQIndexConfig.KEY_NUM_CENTROIDS]?.toInt() ?: IVFPQIndexConfig.DEFAULT_CENTROIDS,
            parameters[IVFPQIndexConfig.KEY_SEED]?.toInt() ?: System.currentTimeMillis().toInt()
        )

        /**
         * Returns the [PQIndexConfig.Binding]
         *
         * @return [PQIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = PQIndexConfig.Binding
    }

    override val type: IndexType = IndexType.PQ

    override fun newTx(context: TransactionContext): IndexTx {
        TODO("Not yet implemented")
    }

    override fun newRebuilder(context: TransactionContext): IndexRebuilder<*> {
        TODO("Not yet implemented")
    }

    override fun newAsyncRebuilder(): AsyncIndexRebuilder<*> {
        TODO("Not yet implemented")
    }
}