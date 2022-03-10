package org.vitrivr.cottontail.dbms.index.lsh.superbit

import jetbrains.exodus.bindings.IntegerBinding
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.ComplexVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.functions.math.distance.Distances
import org.vitrivr.cottontail.dbms.index.AbstractHDIndex
import org.vitrivr.cottontail.dbms.index.AbstractIndex
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.basics.avc.AuxiliaryValueCollection
import org.vitrivr.cottontail.dbms.index.lsh.LSHIndex
import org.vitrivr.cottontail.dbms.operations.Operation

import java.util.*

/**
 * A Super Bit LSH based index in the Cottontail DB data model. Can be used to execute NNS under a [Distances.COSINE]
 * or [Distances.INNERPRODUCT] more efficiently.
 *
 * @author Manuel Huerbin, Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class SuperBitLSHIndex<T : VectorValue<*>>(name: Name.IndexName, parent: DefaultEntity) : LSHIndex<T>(name, parent) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(SuperBitLSHIndex::class.java)
        private val SUPPORTED_DISTANCES = arrayOf(Distances.COSINE.functionName, Distances.INNERPRODUCT.functionName)
    }

    /** False since [SuperBitLSHIndex] doesn't supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** False since [SuperBitLSHIndex] doesn't support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
    override val config: SuperBitLSHIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        SuperBitLSHIndexConfig.fromParamMap(entry.config)
    }

    init {
        require(this.columns.size == 1) { "SuperBitLSHIndex only supports indexing a single column." }
        require(this.columns[0].type is Types.Vector<*,*>) { "SuperBitLSHIndex only support indexing of vector columns." }
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [SuperBitLSHIndex].
     * note: only use the inner product distances with normalized vectors!
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is ProximityPredicate
                && predicate.columns.first() == this.columns[0]
                && predicate.distance.signature.name in SUPPORTED_DISTANCES
                && (!this.config.considerImaginary || predicate.query is ComplexVectorValue<*>)

    /**
     * Calculates the cost estimate of this [SuperBitLSHIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = if (canProcess(predicate)) {
        Cost.ZERO /* TODO: Determine. */
    } else {
        Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [SuperBitLSH] index
     */
    override fun close() {
        /* */
    }

    /**
     * A [IndexTx] that affects this [SuperBitLSHIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
        override val config: SuperBitLSHIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@SuperBitLSHIndex.name, this@SuperBitLSHIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@SuperBitLSHIndex.name}.")
                return SuperBitLSHIndexConfig.fromParamMap(entry.config)
            }

        override val auxiliary: AuxiliaryValueCollection
            get() = TODO("Not yet implemented")

        /**
         * Adds a mapping from the bucket [IntArray] to the given [TupleId].
         *
         * @param signature The [IntArray] signature key to add a mapping for.
         * @param tupleId The [TupleId] to add to the mapping
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(signature: IntArray, tupleId: TupleId): Boolean {
            val keyRaw = IntegerBinding.intToCompressedEntry(0) //TODO
            val tupleIdRaw = tupleId.toKey()
            return if (this.dataStore.exists(this.context.xodusTx, keyRaw, tupleIdRaw)) {
                this.dataStore.put(this.context.xodusTx, keyRaw, tupleIdRaw)
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
        private fun removeMapping(signature: IntArray, tupleId: TupleId): Boolean {
            val keyRaw = IntegerBinding.intToCompressedEntry(0) //TODO
            val valueRaw = tupleId.toKey()
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            return if (cursor.getSearchBoth(keyRaw, valueRaw)) {
                cursor.deleteCurrent()
            } else {
                false
            }
        }

        /**
         * (Re-)builds the [SuperBitLSHIndex].
         */
        override fun rebuild() {
            LOGGER.debug("Rebuilding SB-LSH index {}", this@SuperBitLSHIndex.name)

            /* LSH. */
            val tx = this.context.getTx(this.dbo.parent) as EntityTx
            val specimen = this.acquireSpecimen(tx)
                ?: throw DatabaseException("Could not gather specimen to create index.") // todo: find better exception
            val lsh = SuperBitLSH(this.config.stages, this.config.buckets, this.config.seed, specimen, this.config.considerImaginary, this.config.samplingMethod)


            /* Locally (Re-)create index entries and sort bucket for each stage to corresponding map. */
            val local = List(config.stages) {
                MutableList(config.buckets) { mutableListOf<Long>() }
            }

            /* for every record get bucket-signature, then iterate over stages and add tid to the list of that bucket of that stage */
            tx.cursor(this.columns).forEach {
                val value = it[this.columns[0]] ?: throw DatabaseException("Could not find column for entry in index $this") // todo: what if more columns? This should never happen -> need to change type and sort this out on index creation
                if (value is VectorValue<*>) {
                    val buckets = lsh.hash(value)
                    (buckets zip local).forEach { (bucket, map) ->
                        map[bucket].add(it.tupleId)
                    }
                } else {
                    throw DatabaseException("$value is no vector column!")
                }
            }

            /* Clear existing maps. */
            //TODO: (this@SuperBitLSHIndex.maps zip local).forEach { (map, localdata) ->
            //    map.clear()
            //    localdata.forEachIndexed { bucket, tIds ->
            //        map[bucket] = tIds.toLongArray()
            //   }
            // }

            /* Update state of index. */
            this.updateState(IndexState.CLEAN)
            LOGGER.debug("Rebuilding SB-LSH index completed.")
        }

        override fun tryApply(event: Operation.DataManagementOperation.InsertOperation): Boolean {
            TODO("Not yet implemented")
        }

        override fun tryApply(event: Operation.DataManagementOperation.UpdateOperation): Boolean {
            TODO("Not yet implemented")
        }

        override fun tryApply(event: Operation.DataManagementOperation.DeleteOperation): Boolean {
            TODO("Not yet implemented")
        }

        /**
         * Clears the [SuperBitLSHIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() {
            /* Update state of index. */
            this.updateState(IndexState.STALE)
            // TODO: (this@SuperBitLSHIndex.maps).forEach { map ->
            //    map.clear()
            //}
        }

        /**
         * Performs a lookup through this [SuperBitLSHIndex] and returns a [Cursor] of all [TupleId]s that match the [Predicate].
         * O nly supports [ProximityPredicate]s.
         *
         * The [Cursor] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Cursor<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is ProximityPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** List of [TupleId]s returned by this [Iterator]. */
            private val tupleIds: LinkedList<TupleId>

            /** */
            private var current: TupleId? = null

            /* Performs some sanity checks. */
            init {
                if (this.predicate.columns.first() != this@SuperBitLSHIndex.columns[0] || !(this.predicate.distance.signature.name in SUPPORTED_DISTANCES)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }

                /* Assure correctness of query vector. */
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }

                /** Prepare SuperBitLSH data structure. */
                val config = this@Tx.config
                val lsh = SuperBitLSH(config.stages, config.buckets, config.seed, value, config.considerImaginary, config.samplingMethod)

                /** Prepare list of matches. */
                this.tupleIds = LinkedList<TupleId>()
                val signature = lsh.hash(value)
                for (stage in signature.indices) {
                    //TODO: for (tupleId in this@SuperBitLSHIndex.maps[stage].getValue(signature[stage])) {
                    //    this.tupleIds.offer(tupleId)
                    //}
                }
            }

            override fun next(): Record = StandaloneRecord(this.tupleIds.removeFirst(), this@SuperBitLSHIndex.produces, arrayOf())

            override fun moveNext(): Boolean {
                if (this.tupleIds.isNotEmpty()) {
                    this.current = this.tupleIds.removeFirst()
                    return true
                }
                return false
            }

            override fun key(): TupleId = this.current ?: throw IllegalStateException("Cursor doesn't point ot a valid entry.")

            override fun value(): Record = StandaloneRecord(this.key(), this@SuperBitLSHIndex.produces, arrayOf())

            override fun close() {
                TODO("Not yet implemented")
            }
        }

        /**
         * The [SuperBitLSHIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The SuperBitLSHIndex does not support ranged filtering!")
        }

        /**
         * Tries to find a specimen of the [VectorValue] in the [DefaultEntity] underpinning this [SuperBitLSHIndex]
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