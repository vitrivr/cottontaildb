package org.vitrivr.cottontail.database.index.hash

import org.mapdb.Serializer
import org.mapdb.serializer.SerializerArrayTuple
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.*
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.file.Path
import java.util.*

/**
 * Represents an [AbstractIndex] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 2.0.0
 */
class NonUniqueHashIndex(path: Path, parent: DefaultEntity) : AbstractIndex(path, parent) {
    /**
     * Index-wide constants.
     */
    companion object {
        const val NUQ_INDEX_MAP = "cdb_nuq_map"
    }

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.HASH

    /** True since [NonUniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [NonUniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The [NonUniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** Map structure used for [NonUniqueHashIndex]. */
    private val map: NavigableSet<Array<Any>> =
        this.store.treeSet(
            NUQ_INDEX_MAP,
            SerializerArrayTuple(this.columns[0].type.serializer(), Serializer.LONG_DELTA)
        ).createOrOpen()

    init {
        this.store.commit() /* Initial commit. */
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [NonUniqueHashIndex]. [NonUniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Atomic
            && !predicate.not
            && predicate.columns.first() == this.columns[0]
            && (predicate.operator == ComparisonOperator.IN || predicate.operator == ComparisonOperator.EQUAL)

    /**
     * Calculates the cost estimate of this [NonUniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS, predicate.columns.map { it.type.physicalSize }.sum().toFloat())
        predicate.operator == ComparisonOperator.IN -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.values.size, Cost.COST_MEMORY_ACCESS * predicate.values.size, predicate.columns.map { it.type.physicalSize }.sum().toFloat())
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for..
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * An [IndexTx] that affects this [NonUniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** A per-[Tx] buffer for (de-)serializing values into the set.*/
        private val buffer: Array<Any> = Array(2) { Any() }

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(key: Value, tupleId: TupleId): Boolean {
            this.buffer[0] = key
            this.buffer[1] = tupleId
            this@NonUniqueHashIndex.map.add(this.buffer)
            return true
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         * @param tupleId The [TupleId] to remove.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value, tupleId: TupleId): Boolean {
            this.buffer[0] = key
            this.buffer[1] = tupleId
            return this@NonUniqueHashIndex.map.add(this.buffer)
        }

        /**
         * Returns the number of entries in this [NonUniqueHashIndex.map] which DOES NOT directly
         * correspond to the number of [TupleId]s it encodes.
         *
         * @return Number of [TupleId]s in this [NonUniqueHashIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@NonUniqueHashIndex.map.size.toLong()
        }

        /**
         * (Re-)builds the [NonUniqueHashIndex].
         */
        override fun rebuild() = this.withWriteLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this@NonUniqueHashIndex.map.clear()
            entityTx.scan(this.columns).forEach { record ->
                val value = record[this.columns[0]] ?: throw TxException.TxValidationException(
                    this.context.txId,
                    "A value cannot be null for instances of NonUniqueHashIndex ${this@NonUniqueHashIndex.name} but given value is (value = null, tupleId = ${record.tupleId})."
                )
                this.addMapping(value, record.tupleId)
            }
        }

        /**
         * Updates the [NonUniqueHashIndex] with the provided [Record]. This method determines, whether
         * the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param event [DataChangeEvent] to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            when (event) {
                is DataChangeEvent.InsertDataChangeEvent -> {
                    val value = event.inserts[this.columns[0]]
                    if (value != null) {
                        this.addMapping(value, event.tupleId)
                    }
                }
                is DataChangeEvent.UpdateDataChangeEvent -> {
                    val old = event.updates[this.columns[0]]?.first
                    if (old != null) {
                        this.removeMapping(old, event.tupleId)
                    }
                    val new = event.updates[this.columns[0]]?.second
                    if (new != null) {
                        this.addMapping(new, event.tupleId)
                    }
                }
                is DataChangeEvent.DeleteDataChangeEvent -> {
                    val old = event.deleted[this.columns[0]]
                    if (old != null) {
                        this.removeMapping(old, event.tupleId)
                    }
                }
            }
        }

        /**
         * Performs a lookup through this [NonUniqueHashIndex.Tx] and returns a [Iterator] of
         * all [Record]s that match the [Predicate]. Only supports [ BooleanPredicate.AtomicBooleanPredicate]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {

            /** Local [ BooleanPredicate.AtomicBooleanPredicate] instance. */
            private val predicate: BooleanPredicate.Atomic

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic) { "NonUniqueHashIndex.filter() does only support AtomicBooleanPredicates." }
                require(!predicate.not) { "NonUniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this@Tx.withReadLock { /* No op. */ }
                this.predicate = predicate
            }

            /** Pre-fetched entries that match the [Predicate]. */
            private val elements = LinkedList<Array<Any>>()

            init {
                if (this.predicate.operator == ComparisonOperator.IN) {
                    this.predicate.values.forEach { v ->
                        val subset = this@NonUniqueHashIndex.map.subSet(
                            arrayOf(v),
                            arrayOf(v, (null as Any?)) as Array<Any> /* Safe! */
                        )
                        this.elements.addAll(subset)
                    }
                } else if (this.predicate.operator == ComparisonOperator.EQUAL) {
                    val v = this.predicate.values.first()
                    val subset = this@NonUniqueHashIndex.map.subSet(
                        arrayOf(v),
                        arrayOf(v, (null as Any?)) as Array<Any> /* Safe! */
                    )
                    this.elements.addAll(subset)
                }
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                return this.elements.isNotEmpty()
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                val next = this.elements.poll()
                return StandaloneRecord(
                    next[1] as TupleId,
                    this@Tx.columns,
                    arrayOf(next[0] as Value)
                )
            }
        }

        /**
         * The [NonUniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] to perform the lookup.
         * @param range The [LongRange] to consider.
         * @return The resulting [Iterator].
         */
        override fun filterRange(
            predicate: Predicate,
            range: LongRange
        ): Iterator<Record> {
            throw UnsupportedOperationException("The NonUniqueHashIndex does not support ranged filtering!")
        }
    }
}
