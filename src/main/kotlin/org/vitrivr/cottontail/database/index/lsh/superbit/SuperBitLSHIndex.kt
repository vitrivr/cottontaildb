package org.vitrivr.cottontail.database.index.lsh.superbit

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.lsh.LSHIndex
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.math.knn.metrics.CosineDistance
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Represents a LSH based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.3.0
 */
class SuperBitLSHIndex<T : VectorValue<*>>(name: Name.IndexName, parent: Entity, columns: Array<ColumnDef<*>>, params: Map<String, String>? = null) : LSHIndex<T>(name, parent, columns) {

    companion object {
        const val CONFIG_NAME = "lsh_config"
        const val CONFIG_NAME_STAGES = "stages"
        const val CONFIG_NAME_BUCKETS = "buckets"
        const val CONFIG_NAME_SEED = "seed"
        private const val CONFIG_DEFAULT_STAGES = 3
        private const val CONFIG_DEFAULT_BUCKETS = 10
        private val LOGGER = LoggerFactory.getLogger(SuperBitLSHIndex::class.java)
    }

    /** Internal configuration object for [SuperBitLSHIndex]. */
    val config = this.db.atomicVar(CONFIG_NAME, SuperBitLSHIndexConfig.Serializer).createOrOpen()

    /** True since [SuperBitLSHIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** The [IndexType] of this [SuperBitLSHIndex]. */
    override val type = IndexType.SUPERBIT_LSH

    init {
        if (!columns.all { it.type.vector }) {
            throw DatabaseException.IndexNotSupportedException(name, "Because only vector columns are supported for SuperBitLSHIndex.")
        }
        if (params != null) {
            val buckets = params[CONFIG_NAME_BUCKETS]?.toIntOrNull() ?: CONFIG_DEFAULT_BUCKETS
            val stages = params[CONFIG_NAME_STAGES]?.toIntOrNull() ?: CONFIG_DEFAULT_STAGES
            val seed = params[CONFIG_NAME_SEED]?.toLongOrNull() ?: System.currentTimeMillis()
            this.config.set(SuperBitLSHIndexConfig(buckets, stages, seed))
        }
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [SuperBitLSHIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is KnnPredicate<*>) {
        predicate.columns.first() == this.columns[0] && (predicate.distance is CosineDistance ||predicate.distance is AbsoluteInnerProductDistance)
    } else {
        false
    }

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
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [Index].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {

        /**
         * (Re-)builds the [SuperBitLSHIndex].
         */
        override fun rebuild() = this.withWriteLock {
            LOGGER.trace("Rebuilding SB-LSH index {}", this@SuperBitLSHIndex.name)

            /* LSH. */
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val specimen = this.acquireSpecimen(txn) ?: return
            val lsh = SuperBitLSH(this@SuperBitLSHIndex.config.get().stages, this@SuperBitLSHIndex.config.get().buckets, this.columns[0].logicalSize, this@SuperBitLSHIndex.config.get().seed, specimen)

            /* (Re-)create index entries locally. */
            val local = Array(this@SuperBitLSHIndex.config.get().buckets) { mutableListOf<Long>() }
            txn.scan(this@SuperBitLSHIndex.columns).use { s ->
                s.forEach { record ->
                    val value = record[this.columns[0]]
                    if (value is VectorValue<*>) {
                        val bucket: Int = lsh.hash(value).last()
                        local[bucket].add(record.tupleId)
                    }
                }
            }

            /* Replace existing map. */
            this@SuperBitLSHIndex.map.clear()
            local.forEachIndexed { bucket, list -> this@SuperBitLSHIndex.map[bucket] = list.toLongArray() }
        }

        /**
         * Updates the [SuperBitLSHIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.withWriteLock {
            TODO()
        }

        /**
         * Performs a lookup through this [SuperBitLSHIndex] and returns a [CloseableIterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate) = object : CloseableIterator<Record> {

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            val predicate = if (predicate is KnnPredicate<*>) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /* Performs some sanity checks. */
            init {
                if (this.predicate.columns.first() != this@SuperBitLSHIndex.columns[0] || !(this.predicate.distance is CosineDistance || this.predicate.distance is AbsoluteInnerProductDistance)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }
                this@Tx.withReadLock { /* No op. */ }
            }

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /** [SuperBitLSH] data structure to calculate the bucket index. */
            private val lsh = SuperBitLSH(this@SuperBitLSHIndex.config.get().stages, this@SuperBitLSHIndex.config.get().buckets, this@SuperBitLSHIndex.columns.first().logicalSize, this@SuperBitLSHIndex.config.get().seed, this.predicate.query.first())

            /** List of [TupleId]s returned by this [CloseableIterator]. */
            private val tupleIds = this.predicate.query.mapNotNull {
                this@SuperBitLSHIndex.map[this.lsh.hash(it).last()]
            }.flatMap {
                it.asIterable()
            }.toMutableList()

            override fun hasNext(): Boolean {
                check(!this.closed) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.tupleIds.isNotEmpty()
            }

            override fun next(): Record {
                check(!this.closed) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return StandaloneRecord(this.tupleIds.removeFirst(), this@SuperBitLSHIndex.columns, emptyArray())
            }

            override fun close() {
                if (!this.closed) {
                    this.closed = true
                }
            }
        }

        /** Performs the actual COMMIT operation by rolling back the [IndexTx]. */
        override fun performCommit() {
            this@SuperBitLSHIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [IndexTx]. */
        override fun performRollback() {
            this@SuperBitLSHIndex.db.rollback()
        }

        /**
         * Tries to find a specimen of the [VectorValue] in the [Entity] underpinning this [SuperBitLSHIndex]
         *
         * @param tx [Entity.Tx] used to read from [Entity]
         * @return A specimen of the [VectorValue] that should be indexed.
         */
        private fun acquireSpecimen(tx: EntityTx): VectorValue<*>? {
            for (index in 2L until tx.count()) {
                val read = tx.read(index, this@SuperBitLSHIndex.columns)[this.columns[0]]
                if (read is VectorValue<*>) {
                    return read
                }
            }
            return null
        }
    }
}