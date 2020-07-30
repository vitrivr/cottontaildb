package org.vitrivr.cottontail.database.index.lsh.superbit

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.LSHIndex
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.metrics.CosineDistance
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.name.Name

/**
 * Represents a LSH based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
class SuperBitLSHIndex<T : VectorValue<*>>(name: Name, parent: Entity, columns: Array<ColumnDef<*>>, params: Map<String, String>? = null) : LSHIndex<T>(name, parent, columns) {

    companion object {
        const val CONFIG_NAME = "lsh_config"
        const val CONFIG_NAME_STAGES = "stages"
        const val CONFIG_NAME_BUCKETS = "buckets"
        const val CONFIG_NAME_SEED = "seed"
        private const val CONFIG_DEFAULT_STAGES = 3
        private const val CONFIG_DEFAULT_BUCKETS = 10
        private val LOGGER = LoggerFactory.getLogger(UniqueHashIndex::class.java)
    }

    /** Internal configuration object for [SuperBitLSHIndex]. */
    val config = this.db.atomicVar(CONFIG_NAME, SuperBitLSHIndexConfig.Serializer).createOrOpen()

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
     * Performs a lookup through this [SuperBitLSHIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset {
        if (predicate is KnnPredicate<*>) {
            /* Guard: Only process predicates that are supported. */
            require(this.canProcess(predicate)) { throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lsh-index) does not support the provided predicate.") }

            /* Prepare empty Recordset and LSH object. */
            val recordset = Recordset(this.produces, (predicate.k * predicate.query.size).toLong())
            val lsh = SuperBitLSH(this.config.get().stages, this.config.get().buckets, this.columns.first().logicalSize, this.config.get().seed, predicate.query.first())

            /* Generate record set .*/
            for (i in predicate.query.indices) {
                val query = predicate.query[i]
                val knn = if (predicate.k == 1) {
                    MinSingleSelection<ComparablePair<Long, DoubleValue>>()
                } else {
                    MinHeapSelection<ComparablePair<Long, DoubleValue>>(predicate.k)
                }

                val bucket: Int = lsh.hash(query).last()
                val tupleIds = this.map[bucket]
                if (tupleIds != null) {
                    tupleIds.forEach {
                        val record = tx.read(it)
                        val value = record[predicate.column]
                        if (value is VectorValue<*>) {
                            if (predicate.weights != null) {
                                knn.offer(ComparablePair(it, predicate.distance(query, value, predicate.weights[i])))
                            } else {
                                knn.offer(ComparablePair(it, predicate.distance(query, value)))
                            }
                        }
                    }
                    for (j in 0 until knn.size) {
                        recordset.addRowUnsafe(knn[j].first, arrayOf(knn[j].second))
                    }
                }
            }
            return recordset
        } else {
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
        }
    }

    /** (Re-)builds the [SuperBitLSHIndex]. */
    override fun rebuild(tx: Entity.Tx) {
        /* LSH. */
        val specimen = this.acquireSpecimen(tx) ?: return
        val lsh = SuperBitLSH(this.config.get().stages, this.config.get().buckets, this.columns[0].logicalSize, this.config.get().seed, specimen)

        /* (Re-)create index entries locally. */
        val local = Array(this.config.get().buckets) { mutableListOf<Long>() }
        tx.forEach {
            val value = it[this.columns[0]]
            if (value is VectorValue<*>) {
                val bucket: Int = lsh.hash(value).last()
                local[bucket].add(it.tupleId)
            }
        }

        /* Replace existing map. */
        this.map.clear()
        local.forEachIndexed { bucket, list -> this.map[bucket] = list.toLongArray() }

        /* Commit local database. */
        this.db.commit()
    }

    /**
     * Updates the [SuperBitLSHIndex] with the provided [Record]. This method determines, whether the [Record] should be added or updated
     *
     * @param record Record to update the [SuperBitLSHIndex] with.
     */
    override fun update(update: Collection<DataChangeEvent>, tx: Entity.Tx) = try {
        // TODO
    } catch (e: Throwable) {
        this.db.rollback()
        throw e
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [SuperBitLSHIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is KnnPredicate<*>) {
        predicate.columns.first() == this.columns[0] && predicate.distance is CosineDistance
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
     * Returns true since [SuperBitLSHIndex] supports incremental updates.
     *
     * @return True
     */
    override fun supportsIncrementalUpdate(): Boolean = true

    /**
     * Tries to find a specimen of the [VectorValue] in the [Entity] underpinning this [SuperBitLSHIndex]
     *
     * @param tx [Entity.Tx] used to read from [Entity]
     * @return A specimen of the [VectorValue] that should be indexed.
     */
    private fun acquireSpecimen(tx: Entity.Tx): VectorValue<*>? {
        for (index in 2L until tx.count()) {
            val read = tx.read(index)[this.columns[0]]
            if (read is VectorValue<*>) {
                return read
            }
        }
        return null
    }
}