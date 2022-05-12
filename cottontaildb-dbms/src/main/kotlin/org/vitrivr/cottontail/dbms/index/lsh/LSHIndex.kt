package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.gg.GGIndex
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignature
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignatureGenerator
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQSignature
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
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
         * @param entity The [DefaultEntity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity) = LSHIndex(name, entity)

        /**
         * Initializes the [Store] for a [LSHIndex].
         *
         * @param name The [Name.IndexName] of the [LSHIndex].
         * @param entity The [DefaultEntity.Tx] that executes the operation.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean = try {
            val store = entity.dbo.catalogue.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, entity.context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize LSH index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [LSHIndex].
         *
         * @param name The [Name.IndexName] of the [LSHIndex].
         * @param entity The [DefaultEntity.Tx] that executes the operation.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean = try {
            entity.dbo.catalogue.environment.removeStore(name.storeName(), entity.context.xodusTx)
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
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [LSHIndex] index
     */
    override fun close() {
        /* */
    }

    /**
     * A [IndexTx] that affects this [LSHIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [LSHIndexConfig] used by this [LSHIndex.Tx] instance. */
        override val config: LSHIndexConfig
            get() = super.config as LSHIndexConfig

        /** The Xodus [Store] used to store [PQSignature]s. */
        private var dataStore: Store = this@LSHIndex.catalogue.environment.openStore(this@LSHIndex.name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@LSHIndex.name} is missing.")

        /**
         * Adds a mapping from the bucket [IntArray] to the given [TupleId].
         *
         * @param signature The [IntArray] signature key to add a mapping for.
         * @param tupleId The [TupleId] to add to the mapping
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(signature: LSHSignature, tupleId: TupleId): Boolean {
            val signatureRaw = LSHSignature.Binding.objectToEntry(signature)
            val tupleIdRaw = tupleId.toKey()
            return if (this.dataStore.exists(this.context.xodusTx, signatureRaw, tupleIdRaw)) {
                this.dataStore.put(this.context.xodusTx, signatureRaw, tupleIdRaw)
            } else {
                false
            }
        }

        /**
         * Removes a mapping from the given [IntArray] signature to the given [TupleId].
         *
         * @param signature The [IntArray] signature key to remove a mapping for.
         * @param tupleId The [TupleId] to remove.

         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(signature: LSHSignature, tupleId: TupleId): Boolean {
            val signatureRaw = LSHSignature.Binding.objectToEntry(signature)
            val valueRaw = tupleId.toKey()
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            return if (cursor.getSearchBoth(signatureRaw, valueRaw)) {
                cursor.deleteCurrent()
            } else {
                false
            }
        }

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
         * Checks if this [GGIndex] can process the provided [Predicate] and returns true if so and false otherwise.
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
         * (Re-)builds the [LSHIndex].
         */
        override fun rebuild() {
            LOGGER.debug("Rebuilding LSH index {}", this@LSHIndex.name)

            /* Initialize SignatureGenerator. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx
            val specimen = entityTx.read(entityTx.smallestTupleId(), this@Tx.columns)[this@Tx.columns[0]] as VectorValue<*>

            /* Clears this index. */
            this.clear()

            /* Generate a new LSHSignature for each entry in the entity and adds it to the index. */
            val generator = this.config.generator(specimen.logicalSize)
            val cursor = entityTx.cursor(this.columns)
            cursor.forEach {
                val value = it[this.columns[0]] ?: throw DatabaseException("Could not find column for entry in index $this") // todo: what if more columns? This should never happen -> need to change type and sort this out on index creation
                if (value is VectorValue<*>) {
                    val signature = generator.generate(value)
                    this.addMapping(signature, it.tupleId)
                }
            }

            /* Close cursor. */
            cursor.close()

            /* Update state of index. */
            this.updateState(IndexState.CLEAN, this.config.copy(generator = generator))
        }

        /**
         * Always throws an [UnsupportedOperationException], since [LSHIndex] does not support asynchronous rebuilds.
         */
        override fun asyncRebuild() = throw UnsupportedOperationException("LSHIndex does not support asynchronous rebuild.")

        /**
         * Tries to apply the change applied by this [DataEvent.Insert] to the [LSHIndex] underlying this [LSHIndex.Tx].
         *
         * This method implements the [LSHIndex]'es write model. TODO: True for all types of LSH algorithms?
         *
         * @param event The [DataEvent.Insert] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean = this.txLatch.withLock {
            val generator = this.config.generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = event.data[this.columns[0]]
            check(value is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.addMapping(generator.generate(value), event.tupleId)
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [LSHIndex] underlying this [LSHIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel].  TODO: True for all types of LSH algorithms?
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val generator = this.config.generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = event.data[this.columns[0]]
            check(value?.first is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            check(value?.second is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.removeMapping(generator.generate(value!!.first as VectorValue<*>), event.tupleId) && this.addMapping(generator.generate(value.second as VectorValue<*>), event.tupleId)

        }

        /**
         * Tries to apply the change applied by this [DataEvent.Delete] to the [LSHIndex] underlying this [LSHIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel]: DELETES can always be applied.
         *
         * @param event The [DataEvent.Delete] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Delete): Boolean {
            val generator = this.config.generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = event.data[this.columns[0]]
            check(value is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.removeMapping(generator.generate(value), event.tupleId)
        }

        /**
         * Returns the number of entries in this [LSHIndex].
         *
         * @return Number of entries in this [LSHIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.xodusTx)
        }

        /**
         * Clears the [LSHIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            /* Truncate and replace store.*/
            this@LSHIndex.catalogue.environment.truncateStore(this@LSHIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@LSHIndex.catalogue.environment.openStore(this@LSHIndex.name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@LSHIndex.name} is missing.")

            /* Update catalogue entry for index. */
            this.updateState(IndexState.STALE)
        }

        /**
         * Performs a lookup through this [LSHIndex] and returns a [Cursor] of all [TupleId]s that match the [ProximityPredicate].
         *
         * The resulting [Cursor] is not thread safe!
         *
         * @param predicate The [ProximityPredicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Cursor<Record> {

            /** Cast [ProximityPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is ProximityPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@LSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** Sub transaction for this [Cursor]. */
            private val subTx = this@Tx.context.xodusTx.readonlySnapshot

            /** The Xodus cursors used to navigate the data. */
            private val cursor = this@Tx.dataStore.openCursor(this.subTx)

            /* Performs some sanity checks. */
            init {
                val config = this@Tx.config
                if (this.predicate.columns.first() != this@Tx.columns[0] || this.predicate.distance.name != config.distance) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@LSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }

                /* Assure correctness of query vector. */
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }

                /* Obtain LSH signature of query and set signature. */
                val signature = config.generator?.generate(value)
                check(signature != null) { "Failed to generate signature for query vector." }
                this.cursor.getSearchKey(LSHSignature.Binding.objectToEntry(signature))
            }

            /**
             * Moves this [Cursor] by one entry.
             *
             * Returns true upon success and false if there is no entry left.
             */
            override fun moveNext(): Boolean = this.cursor.nextDup

            /**
             * Returns the next [Record] value.
             *
             * @return Next [Record]
             */
            override fun next(): Record = this.value()

            /**
             * Returns the next [Record] value.
             *
             * @return Next [Record]
             */
            override fun value(): Record = StandaloneRecord(this.key(), emptyArray(), emptyArray())

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
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> {
            throw UnsupportedOperationException("The LSHIndex does not support ranged filtering!")
        }
    }
}