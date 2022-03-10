package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.LongBinding
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.dbms.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.dbms.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.basics.avc.AuxiliaryValueCollection
import org.vitrivr.cottontail.dbms.index.va.bounds.Bounds
import org.vitrivr.cottontail.dbms.index.va.bounds.L1Bounds
import org.vitrivr.cottontail.dbms.index.va.bounds.L2Bounds
import org.vitrivr.cottontail.dbms.index.va.bounds.L2SBounds
import org.vitrivr.cottontail.dbms.index.va.signature.VAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.statistics.columns.DoubleVectorValueStatistics
import org.vitrivr.cottontail.dbms.statistics.columns.FloatVectorValueStatistics
import org.vitrivr.cottontail.dbms.statistics.columns.IntVectorValueStatistics
import org.vitrivr.cottontail.dbms.statistics.columns.LongVectorValueStatistics
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import org.vitrivr.cottontail.utilities.selection.HeapSelection
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.lang.Math.floorDiv
import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * An [AbstractHDIndex] structure for proximity based search (NNS / FNS) that uses a vector
 * approximation (VA) file ([1]). Can be used for all types of [RealVectorValue]s and all
 * Minkowski metrics (L1, L2 etc.).
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class VAFIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)

        /** Key to read/write the Marks entry. */
        private val MARKS_ENTRY_KEY = LongBinding.longToCompressedEntry(0L)
    }

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(KnnUtilities.distanceColumnDef(this.parent.name))

    /** The type of index. */
    override val type = IndexType.VAF

    /** The [VAFIndexConfig] used by this [VAFIndex] instance. */
    override val config: VAFIndexConfig
        get() = this.catalogue.environment.computeInTransaction { tx ->
            val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
            VAFIndexConfig.fromParamMap(entry.config)
        }

    /** False since [VAFIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [VAFIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = false

    /**
     * Calculates the cost estimate if this [VAFIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost {
        if (predicate !is ProximityPredicate) return Cost.INVALID
        if (predicate.column != this.columns[0]) return Cost.INVALID
        if (predicate.distance !is MinkowskiDistance<*>) return Cost.INVALID
        return (Cost.DISK_ACCESS_READ * (0.9f + 0.1f * this.columns[0].type.physicalSize) +
                (Cost.MEMORY_ACCESS * 2.0f + Cost.FLOP) * 0.9f + predicate.cost * 0.1f) * this.count
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is ProximityPredicate && predicate.column == this.columns[0]

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [VAFIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [VAFIndex]
     */
    override fun close() { /* No op. */ }

    /**
     * A [IndexTx] that affects this [VAFIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** Internal [VAFMarks] reference. */
        private var marks: VAFMarks


        /** The configuration map used for the [Index] that underpins this [IndexTx]. */
        override val config: VAFIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@VAFIndex.name, this@VAFIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@VAFIndex.name}.")
                return VAFIndexConfig.fromParamMap(entry.config)
            }

        /** The [AuxiliaryValueCollection] used by this [VAFIndex.Tx]. */
        override val auxiliary: AuxiliaryValueCollection
            get() = TODO("Not yet implemented")

        /** The component-wise minimum vector covered by the [VAFIndex] underpinning this [VAFIndex.Tx]. */
        private val min = DoubleArray(columns[0].type.logicalSize)

        /** The component-wise maximum vector covered by the [VAFIndex] underpinning this [VAFIndex.Tx]. */
        private val max = DoubleArray(columns[0].type.logicalSize)

        init {
            /* Load marks entry object. */
            val rawMarksEntry = this.dataStore.get(this.context.xodusTx, MARKS_ENTRY_KEY)
            if (rawMarksEntry == null) {
                this.marks = VAFMarks.getEquidistantMarks(DoubleArray(this@VAFIndex.dimension), DoubleArray(this@VAFIndex.dimension), IntArray(this@VAFIndex.dimension) { this.config.marksPerDimension })
            } else {
                this.marks = VAFMarks.entryToObject(rawMarksEntry) as VAFMarks
            }

            /* Prepare transaction for entity. */
            val entityTx = this.context.getTx(this@VAFIndex.parent) as EntityTx
            val columnTx = this.context.getTx(entityTx.columnForName(this.columns[0].name)) as ColumnTx<*>
            when (val stat = columnTx.statistics()) {
                is FloatVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                is DoubleVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it]
                    max[it] = stat.max.data[it]
                }
                is IntVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                is LongVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                else -> throw DatabaseException.DataCorruptionException("Unsupported statistics type.")
            }
        }

        /**
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.txLatch.withLock {
            LOGGER.debug("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Calculate and update marks. */
            this.marks = VAFMarks.getEquidistantMarks(this.min, this.max, IntArray(this.columns[0].type.logicalSize) { this.config.marksPerDimension })
            this.dataStore.put(this.context.xodusTx, MARKS_ENTRY_KEY, VAFMarks.objectToEntry(this.marks))

            /* Calculate and update signatures. */
            this.clear()
            val entityTx = this.context.getTx(this@VAFIndex.parent) as EntityTx
            entityTx.cursor(this.columns).forEach { r ->
                val value = r[this.columns[0]]
                if (value is RealVectorValue<*>) {
                    this.dataStore.put(this.context.xodusTx, r.tupleId.toKey(), VAFSignature.valueToEntry(this.marks.getSignature(value)))
                }
            }

            /* Update catalogue entry for index. */
            this.updateState(IndexState.CLEAN)
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.InsertOperation] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method
         * implements the [VAFIndex]'es write model: INSERTS can be applied, if inserted vector lies within the grid obtained upon creation of the index.
         *
         * @param operation The [Operation.DataManagementOperation.InsertOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.InsertOperation): Boolean {
            val value = operation.inserts[this@VAFIndex.column]
            require(value is RealVectorValue<*>) { "Only real vector values can be stored in a VAFIndex. This is a programmer's error!" }
            for (i in value.indices) {
                if (value[i].value.toDouble() < this.min[i] || value[i].value.toDouble() > this.max[i])
                    return false
            }
            return this.dataStore.add(this.context.xodusTx, operation.tupleId.toKey(), VAFSignature.valueToEntry(this.marks.getSignature(value)))
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.UpdateOperation] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel]: DELETES can always be applied.
         *
         * @param operation The [Operation.DataManagementOperation.UpdateOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.UpdateOperation): Boolean {
            val value = operation.updates[this@VAFIndex.column]?.second
            require(value is RealVectorValue<*>) { "Only real vector values can be stored in a VAFIndex. This is a programmer's error!" }
            for (i in value.indices) {
                if (value[i].value.toDouble() < this.min[i] || value[i].value.toDouble() > this.max[i])
                    return false
            }
            return this.dataStore.put(this.context.xodusTx, operation.tupleId.toKey(), VAFSignature.valueToEntry(this.marks.getSignature(value)))
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.UpdateOperation] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel]: UPDATES can be applied, if updated vector lies within the grid obtained upon creation of the index.
         *
         * @param operation The [Operation.DataManagementOperation.DeleteOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.DeleteOperation): Boolean {
            return this.dataStore.delete(this.context.xodusTx, operation.tupleId.toKey())
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate]. Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = filterRange(predicate, 0, 1)

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] within the given [LongRange]. Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int) = object : Cursor<Record> {

            /** Cast to [ProximityPredicate] (if such a cast is possible).  */
            private val predicate = predicate as ProximityPredicate

            /** [VectorValue] used for query. Must be prepared before using the [Iterator]. */
            private val query: RealVectorValue<*>

            /** The [VAFMarks] used by this [Iterator]. */
            private val marks: VAFMarks = this@Tx.marks

            /** The [Bounds] objects used for filtering. */
            private val bounds: Bounds

            /** Internal [EntityTx] used to access actual values. */
            private val entityTx = this@Tx.context.getTx(this@VAFIndex.parent) as EntityTx

            /** The [MinHeapSelection] use for finding the top k entries. */
            private var selection = HeapSelection(this.predicate.k.toLong(), RecordComparator.SingleNonNullColumnComparator(this@VAFIndex.produces[0], SortOrder.ASCENDING))

            /** The [TupleId] range this [Cursor] covers. */
            private val range: LongRange

            /** The current [Cursor] position. */
            private var position = -1L

            init {
                val value = this.predicate.query.value
                check(value is RealVectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this.query = value
                this.bounds = when (this.predicate.distance) {
                    is ManhattanDistance<*> -> L1Bounds(this.query, this.marks)
                    is EuclideanDistance<*> -> L2Bounds(this.query, this.marks)
                    is SquaredEuclideanDistance<*> -> L2SBounds(this.query, this.marks)
                    else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
                }

                /* Calculate partition size and create iterator. */
                val maximumTupleId = this.entityTx.maxTupleId()
                val partitionSize = floorDiv(maximumTupleId, partitions) + 1
                this.range = (partitionSize * partitionIndex) until min(partitionSize * (partitionIndex + 1), maximumTupleId)

                /* Prepares the result set. */
                this.prepare()
            }

            /**
             * Moves the internal cursor and return true, as long as new candidates appear.
             */
            override fun moveNext(): Boolean = if (this.position < this.selection.size - 1L) {
                this.position += 1L
                true
            } else {
                false
            }

            /**
             * Returns the current [TupleId] this [Cursor] is pointing to.
             *
             * @return [TupleId]
             */
            override fun key(): TupleId = this.selection[this.position].tupleId

            /**
             * Returns the current [Record] this [Cursor] is pointing to.
             *
             * @return [TupleId]
             */
            override fun value(): Record = this.selection[this.position]

            /**
             * Closes this [Cursor]
             */
            override fun close() { }

            /**
             * Prepares the result set using the [VAFIndex].
             */
            private fun prepare() {
                var read = 0L
                val subTx = this@Tx.context.xodusTx.readonlySnapshot
                for (tupleId in this.range) {
                    /** Check if index contains an entry for tupleId. If so, use signature to determine whether distance must be calculated. */
                    val signature = this@Tx.dataStore.get(subTx, tupleId.toKey())
                    if (signature != null) {
                        if (this.selection.size > this.predicate.k && !this.bounds.isVASSACandidate(VAFSignature.entryToValue(signature), (this.selection.peek()!![0] as DoubleValue).value)) {
                            continue /* Skip if signature estimate does make clear that no calculation is required. */
                        }
                    }

                    /** Check if entity contains an entry for tupleId. If not, skip it!*/
                    if (!this.entityTx.contains(tupleId)) {
                        continue
                    }

                    /** Now read and calculate distance. */
                    read += 1
                    val value = this.entityTx.read(tupleId, this@VAFIndex.columns)[this@VAFIndex.columns[0]]
                    if (value is VectorValue<*>) {
                        val distance =  this.predicate.distance(this.query, value)
                        if (distance != null) {
                            this.selection.offer(StandaloneRecord(tupleId, this@VAFIndex.produces, arrayOf(value, distance)))
                        }
                    }
                }

                /* Commit sub transaction. */
                subTx.commit()
            }
        }
    }
}