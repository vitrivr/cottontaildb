package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignature
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignatureGenerator
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.concurrent.withLock

/**
 * An [AbstractIndex] structure for proximity based search (NNS / FNS) based on locality sensitive hashing (LSH, see [1]).
 *
 * This [LSHIndex] is a generalization that basically maps an [LSHSignature] to the [TupleId] that match that [LSHSignature].
 * Generating the [LSHSignature] is delegated to a [LSHSignatureGenerator], which enables different types of LSH algorithms
 * specific for certain [VectorDistance].
 *
 * References:
 * [1] Indyk, P. and Motwani, R., 1998. Approximate Nearest Neighbors: Towards Removing the Curse of Dimensionality (p. 604–613). Proceedings of the Thirtieth Annual ACM Symposium on Theory of Computing
 *
 * @author Ralph Gasser, Manuel Hürbin, Gabriel Zihlmann
 * @version 1.1.0
 */
class LSHIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [LSHIndex].
     */
    companion object: IndexDescriptor<LSHIndex> {

        /** The [Logger] instance used by [LSHIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(LSHIndex::class.java)

        /** True since [LSHIndex] supports incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [LSHIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = false

        /** False since [LSHIndex] does not support partitioning. */
        override val supportsPartitioning: Boolean = false

        /**
         * Opens a [PQIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [Entity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity) = LSHIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [LSHIndex].
         *
         * @param name The [Name.IndexName] of the [LSHIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [Transaction] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: Transaction): Boolean = try {
            val store = catalogue.transactionManager.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize LSH index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [LSHIndex].
         *
         * @param name The [Name.IndexName] of the [LSHIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [Transaction] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: Transaction): Boolean = try {
            catalogue.transactionManager.environment.removeStore(name.storeName(), context.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize LSH index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns a [LSHIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [LSHIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<LSHIndex> = LSHIndexConfig(
            distance = parameters[LSHIndexConfig.KEY_DISTANCES]?.let { Name.FunctionName(it) } ?: LSHIndexConfig.DEFAULT_DISTANCE,
            buckets = parameters[LSHIndexConfig.KEY_NUM_BUCKETS]?.toInt() ?: 50,
            stages = parameters[LSHIndexConfig.KEY_NUM_STAGES]?.toInt() ?: 5,
            seed = parameters[LSHIndexConfig.KEY_SEED]?.toLong() ?: System.currentTimeMillis()
        )

        /**
         * Returns the [LSHIndexConfig.Binding]
         *
         * @return [LSHIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = LSHIndexConfig.Binding
    }

    /** The [IndexType] of this [LSHIndex]. */
    override val type: IndexType = IndexType.LSH

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [LSHIndex].
     *
     * @param context The [QueryContext] to create this [IndexTx] for.
     */
    override fun newTx(context: QueryContext): IndexTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * Opens and returns a new [LSHIndexRebuilder] object that can be used to rebuild with this [LSHIndex].
     *
     * @param context The [QueryContext] to create [LSHIndexRebuilder] for.
     * @return [LSHIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext) = LSHIndexRebuilder(this, context)

    /**
     * Since [LSHIndex] does not support asynchronous re-indexing, this method will throw an error.
     */
    override fun newAsyncRebuilder(context: QueryContext): AsyncIndexRebuilder<*>
        = throw UnsupportedOperationException("LSHIndex does not support asynchronous index rebuilding.")

    /**
     * A [IndexTx] that affects this [LSHIndex].
     */
    private inner class Tx(context: QueryContext) : AbstractIndex.Tx(context) {

        /** The Xodus [Store] used to store [LSHSignature]s. */
        private val store: LSHDataStore = LSHDataStore.open(this.context.txn.xodusTx, this@LSHIndex)

        /**
         * [LSHIndex] only produced candidate [TupleId]s and no columns.
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> {
            require(predicate is ProximityPredicate) { "LSHIndex can only process proximity predicates." }
            return emptyList()
        }

        /**
         * Checks if this [LSHIndex] can process the provided [Predicate] and returns true if so and false otherwise.
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean
           = predicate is ProximityPredicate && predicate.column == this.columns[0] && predicate.distance is CosineDistance<*>

        /**
         * Returns the map of [Trait]s this [LSHIndex] implements for the given [Predicate]s.
         *
         * @param predicate [Predicate] to check.
         * @return Map of [Trait]s for this [LSHIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = when (predicate) {
            is ProximityPredicate.NNS -> mutableMapOf(
                OrderTrait to OrderTrait(listOf(predicate.distanceColumn to SortOrder.ASCENDING)),
                LimitTrait to LimitTrait(predicate.k),
                NotPartitionableTrait to NotPartitionableTrait
            )
            is ProximityPredicate.FNS -> mutableMapOf(
                OrderTrait to OrderTrait(listOf(predicate.distanceColumn to SortOrder.DESCENDING)),
                LimitTrait to LimitTrait(predicate.k),
                NotPartitionableTrait to NotPartitionableTrait
            )
            else -> throw IllegalArgumentException("Unsupported predicate for high-dimensional index. This is a programmer's error!")
        }

        /**
         * Estimates the [Cost] for using this [LSHIndex] to evaluate the given [Predicate]
         *
         * @param predicate [Predicate] to check.
         * @return [Cost] estimation.
         */
        override fun costFor(predicate: Predicate): Cost {
            TODO("Not yet implemented")
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Insert] to the [LSHIndex] underlying this [LSHIndex.Tx].
         *
         * This method implements the [LSHIndex]'es write model. TODO: True for all types of LSH algorithms?
         *
         * @param event The [DataEvent.Insert] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean = this.txLatch.withLock {
            val generator = (this.config as LSHIndexConfig).generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = event.data[this.columns[0]]
            check(value is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.store.addMapping(this.context.txn.xodusTx, generator.generate(value), event.tupleId)
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [LSHIndex] underlying this [LSHIndex.Tx]. This method
         * implements the [LSHIndex]'es [WriteModel].  TODO: True for all types of LSH algorithms?
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val generator = (this.config as LSHIndexConfig).generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = event.data[this.columns[0]]
            check(value?.first is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            check(value?.second is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.store.removeMapping(this.context.txn.xodusTx, generator.generate(value!!.first as VectorValue<*>), event.tupleId) && this.store.addMapping(this.context.txn.xodusTx,generator.generate(value.second as VectorValue<*>), event.tupleId)
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Delete] to the [LSHIndex] underlying this [LSHIndex.Tx]. This method
         * implements the [LSHIndex]'es [WriteModel]: DELETES can always be applied.
         *
         * @param event The [DataEvent.Delete] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Delete): Boolean {
            val generator = (this.config as LSHIndexConfig).generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = event.data[this.columns[0]]
            check(value is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.store.removeMapping(this.context.txn.xodusTx, generator.generate(value), event.tupleId)
        }

        /**
         * Returns the number of entries in this [LSHIndex].
         *
         * @return Number of entries in this [LSHIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.store.store.count(this.context.txn.xodusTx)
        }

        /**
         * Performs a lookup through this [LSHIndex] and returns a [Cursor] of all [TupleId]s that match the [ProximityPredicate].
         *
         * The resulting [Cursor] is not thread safe!
         *
         * @param predicate The [ProximityPredicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Cursor<Tuple> {

            /** Cast [ProximityPredicate] (if such a cast is possible).  */
            private val predicate: ProximityPredicate = if (predicate is ProximityPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@LSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** Sub transaction for this [Cursor]. */
            private val subTx = this@Tx.context.txn.xodusTx.readonlySnapshot

            /** The Xodus cursors used to navigate the data. */
            private val cursor = this@Tx.store.store.openCursor(this.subTx)

            /* Performs some sanity checks. */
            init {
                val config = this@Tx.config
                if (this.predicate.columns.first() != this@Tx.columns[0] || this.predicate.distance.name != (config as LSHIndexConfig).distance) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@LSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }

                /* Assure correctness of query vector. */
                with(MissingTuple) {
                    with(this@Tx.context.bindings) {
                        val value = (predicate as ProximityPredicate).query.getValue()
                        check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }

                        /* Obtain LSH signature of query and set signature. */
                        val signature = config.generator?.generate(value)
                        check(signature != null) { "Failed to generate signature for query vector." }
                        cursor.getSearchKey(LSHSignature.Binding.objectToEntry(signature))
                    }
                }
            }

            /**
             * Moves this [Cursor] by one entry.
             *
             * Returns true upon success and false if there is no entry left.
             */
            override fun moveNext(): Boolean = this.cursor.nextDup

            /**
             * Returns the next [Tuple] value.
             *
             * @return Next [Tuple]
             */
            override fun next(): Tuple = this.value()

            /**
             * Returns the next [Tuple] value.
             *
             * @return Next [Tuple]
             */
            override fun value(): Tuple = StandaloneTuple(this.key(), emptyArray(), emptyArray())

            /**
             * Returns the next [TupleId].
             *
             * @return Next [TupleId]
             */
            override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)

            /**
             * Closes the internal Xodus [Cursor] and finalizes the sub transaction.
             */
            override fun close() {
                this.cursor.close()
                this.subTx.commit()
            }
        }

        /**
         * The [LSHIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Cursor].
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
            throw UnsupportedOperationException("The LSHIndex does not support ranged filtering!")
        }
    }
}