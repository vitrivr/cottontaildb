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
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignature
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignatureGenerator
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQSignature
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import kotlin.concurrent.withLock

/**
 * An [AbstractHDIndex] structure for proximity based search (NNS / FNS) based on locality sensitive hashing (LSH, see [1]).
 *
 * This [LSHIndex] is a generalization that basically maps an [LSHSignature] to the [TupleId] that match that [LSHSignature].
 * Generating the [LSHSignature] is delegated to a [LSHSignatureGenerator], which enables different types of LSH algorithms
 * specific for certain [VectorDistance].
 *
 * References:
 * [1] Indyk, P. and Motwani, R., 1998. Approximate Nearest Neighbors: Towards Removing the Curse of Dimensionality (p. 604–613). Proceedings of the Thirtieth Annual ACM Symposium on Theory of Computing
 *
 * @author Ralph Gasser, Manuel Hürbin, Gabriel Zihlmann
 * @version 1.0.0
 */
class LSHIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [LSHIndex].
     */
    companion object: IndexDescriptor<LSHIndex> {

        /** The [Logger] instance used by [LSHIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(LSHIndex::class.java)

        /**
         * Opens a [PQIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [DefaultEntity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity) = LSHIndex(name, entity)

        /**
         * Tries to initialize the [Store] for a [LSHIndex].
         *
         * @param name The [Name.IndexName] of the [LSHIndex].
         * @param entity The [DefaultEntity] that holds the [LSHIndex].
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean {
            val store = entity.dbo.catalogue.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, entity.context.xodusTx, true)
            return store != null
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

    /** The [LSHIndexConfig] used by this [LSHIndex] instance. */
    override val config: LSHIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        entry.config as LSHIndexConfig
    }

    /** The [IndexType] of this [LSHIndex]. */
    override val type: IndexType = IndexType.LSH

    /** True since [LSHIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False since [LSHIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [LSHIndex].
     * note: only use the inner product distances with normalized vectors!
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is ProximityPredicate
            && predicate.columns.first() == this.columns[0]
            && predicate.distance.signature.name in LSHIndexConfig.SUPPORTED_DISTANCES

    /**
     * Returns a [List] of the [ColumnDef] produced by this [LSHIndex].
     *
     * @return [List] of [ColumnDef].
     */
    override fun produces(predicate: Predicate): List<ColumnDef<*>> {
        require(predicate is ProximityPredicate) { "LSHIndex can only process proximity predicates." }
        return listOf()
    }

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
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** The [LSHIndexConfig] used by this [LSHIndex.Tx] instance. */
        override val config: LSHIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@LSHIndex.name, this@LSHIndex.catalogue, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@LSHIndex.name}.")
                return entry.config as LSHIndexConfig
            }

        override fun cost(predicate: Predicate): Cost {
            TODO("Not yet implemented")
        }

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
         * (Re-)builds the [LSHIndex].
         */
        override fun rebuild() {
            LOGGER.debug("Rebuilding SB-LSH index {}", this@LSHIndex.name)

            /* Initialize SignatureGenerator. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx
            val specimen = this.acquireSpecimen(entityTx)
            if (specimen != null) {
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
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.InsertOperation] to the [LSHIndex] underlying this [LSHIndex.Tx].
         *
         * This method implements the [LSHIndex]'es write model. TODO: True for all types of LSH algorithms?
         *
         * @param operation The [Operation.DataManagementOperation.InsertOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.InsertOperation): Boolean = this.txLatch.withLock {
            val generator = this.config.generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = operation.inserts[this.dbo.columns[0]]
            check(value is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.addMapping(generator.generate(value), operation.tupleId)
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.UpdateOperation] to the [LSHIndex] underlying this [LSHIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel].  TODO: True for all types of LSH algorithms?
         *
         * @param operation The [Operation.DataManagementOperation.UpdateOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.UpdateOperation): Boolean {
            val generator = this.config.generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = operation.updates[this.dbo.columns[0]]
            check(value?.first is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            check(value?.second is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.removeMapping(generator.generate(value!!.first as VectorValue<*>), operation.tupleId) && this.addMapping(generator.generate(value.second as VectorValue<*>), operation.tupleId)

        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.DeleteOperation] to the [LSHIndex] underlying this [LSHIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel]: DELETES can always be applied.
         *
         * @param operation The [Operation.DataManagementOperation.DeleteOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.DeleteOperation): Boolean {
            val generator = this.config.generator ?: throw IllegalStateException("Failed to obtain LSHSignatureGenerator for index ${this@LSHIndex.name}. This is a programmer's error!")
            val value = operation.deleted[this.dbo.columns[0]]
            check(value is VectorValue<*>) { "Failed to add $value to LSHIndex. Incoming value is not a vector! This is a programmer's error!"}
            return this.removeMapping(generator.generate(value), operation.tupleId)
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
                if (this.predicate.columns.first() != this@LSHIndex.columns[0] || this.predicate.distance.name != config.distance) {
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
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The LSHIndex does not support ranged filtering!")
        }

        /**
         * Tries to find a specimen of the [VectorValue] in the [DefaultEntity] underpinning this [LSHIndex]
         *
         * @param tx [DefaultEntity.Tx] used to read from [DefaultEntity]
         * @return A specimen of the [VectorValue] that should be indexed.
         */
        private fun acquireSpecimen(tx: EntityTx): VectorValue<*>? {
            for (index in 0L until tx.maxTupleId()) {
                val read = tx.read(index, this@Tx.columns)[this@Tx.columns[0]]
                if (read is VectorValue<*>) {
                    return read
                }
            }
            return null
        }
    }
}