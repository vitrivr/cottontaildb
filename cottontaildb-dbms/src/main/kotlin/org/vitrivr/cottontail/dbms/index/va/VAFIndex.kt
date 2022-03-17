package org.vitrivr.cottontail.dbms.index.va

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
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.index.*
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

    /**
     * The [IndexDescriptor] for the [VAFIndex].
     */
    companion object: IndexDescriptor<VAFIndex> {
        /** [Logger] instance used by [VAFIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)

        /**
         * Opens a [VAFIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param entity The [DefaultEntity] that holds the [VAFIndex].
         * @return The opened [VAFIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity): VAFIndex = VAFIndex(name, entity)

        /**
         * Tries to initialize the [Store] for a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param entity The [DefaultEntity] that holds the [VAFIndex].
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean {
            val store = entity.dbo.catalogue.environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, entity.context.xodusTx, true)
            return store != null
        }

        /**
         * Generates and returns a [VAFIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [VAFIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<VAFIndex>
            = VAFIndexConfig(parameters[VAFIndexConfig.KEY_MARKS_PER_DIMENSION]?.toIntOrNull() ?: 10)

        /**
         * Returns the [VAFIndexConfig.Binding]
         *
         * @return [VAFIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = VAFIndexConfig.Binding
    }

    /** The [IndexType] of this [VAFIndex]. */
    override val type
        get() = IndexType.VAF

    /** The [VAFIndexConfig] used by this [VAFIndex] instance. */
    override val config: VAFIndexConfig
        get() = this.catalogue.environment.computeInTransaction { tx ->
            val entry = IndexCatalogueEntry.read(this.name, this.catalogue, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read configuration entry for index ${this.name}.")
            entry.config as VAFIndexConfig
        }

    /** Results produced by [VAFIndex] are always returned sorted by distance in ascending order.  */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>>
        get() = arrayOf(KnnUtilities.distanceColumnDef(this.parent.name) to SortOrder.ASCENDING)

    /** False since [VAFIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [VAFIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = false

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is ProximityPredicate && predicate.column == this.columns[0]

    /**
     * Returns a [List] of the [ColumnDef] produced by this [VAFIndex].
     *
     * @return [List] of [ColumnDef].
     */
    override fun produces(predicate: Predicate): List<ColumnDef<*>> {
        require(predicate is ProximityPredicate) { "VAFIndex can only process proximity predicates." }
        return listOf(predicate.distanceColumn, this.column)
    }

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

        /** The configuration map used for the [Index] that underpins this [IndexTx]. */
        override val config: VAFIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@VAFIndex.name, this@VAFIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@VAFIndex.name}.")
                return entry.config as VAFIndexConfig
            }

        /** The [VAFMarks] object used by this [VAFIndex.Tx]. */
        private val marks: VAFMarks?
            get() = this.config.marks

        /** The Xodus [Store] used to store [VAFSignature]s. */
        private var dataStore: Store = this@VAFIndex.catalogue.environment.openStore(this@VAFIndex.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@VAFIndex.name} is missing.")

        /**
         * Calculates the cost estimate of this [VAFIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun cost(predicate: Predicate): Cost {
            if (predicate !is ProximityPredicate) return Cost.INVALID
            if (predicate.column != this.columns[0]) return Cost.INVALID
            if (predicate.distance !is MinkowskiDistance<*>) return Cost.INVALID
            return (Cost.DISK_ACCESS_READ * (0.9f + 0.1f * this.columns[0].type.physicalSize) +
                    (Cost.MEMORY_ACCESS * 2.0f + Cost.FLOP) * 0.9f + predicate.cost * 0.1f) * this.count()
        }

        /**
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.txLatch.withLock {
            LOGGER.debug("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Obtain component-wise minimum and maximum for the vector held by the entity. */
            val config = this.config
            val indexedColumn = this.columns[0]
            val dimension = indexedColumn.type.logicalSize
            val entityTx = this.context.getTx(this@VAFIndex.parent) as EntityTx
            val columnTx = this.context.getTx(entityTx.columnForName(this.columns[0].name)) as ColumnTx<*>
            val minimum = when (val stat = columnTx.statistics()) {
                is FloatVectorValueStatistics -> DoubleArray(dimension) { stat.min.data[it].toDouble() }
                is DoubleVectorValueStatistics -> DoubleArray(dimension) {  stat.min.data[it] }
                is IntVectorValueStatistics -> DoubleArray(dimension) { stat.min.data[it].toDouble() }
                is LongVectorValueStatistics -> DoubleArray(dimension) { stat.min.data[it].toDouble() }
                else -> throw DatabaseException.DataCorruptionException("Unsupported statistics type.")
            }
            val maximum = when (val stat = columnTx.statistics()) {
                is FloatVectorValueStatistics -> DoubleArray(dimension) { stat.max.data[it].toDouble() }
                is DoubleVectorValueStatistics -> DoubleArray(dimension) {  stat.max.data[it] }
                is IntVectorValueStatistics -> DoubleArray(dimension) { stat.max.data[it].toDouble() }
                is LongVectorValueStatistics -> DoubleArray(dimension) { stat.max.data[it].toDouble() }
                else -> throw DatabaseException.DataCorruptionException("Unsupported statistics type.")
            }

            /* Calculate and update marks. */
            val newMarks = VAFMarks.getEquidistantMarks(minimum, maximum, config.marksPerDimension)

            /* Clear old signatures. */
            this.clear()

            /* Iterate over entity and update index with entries. */
            val cursor = entityTx.cursor(arrayOf(indexedColumn))
            cursor.forEach { r ->
                val value = r[indexedColumn]
                if (value is RealVectorValue<*>) {
                    this.dataStore.put(this.context.xodusTx, r.tupleId.toKey(), VAFSignature.Binding.valueToEntry(newMarks.getSignature(value)))
                }
            }

            /* Close cursor. */
            cursor.close()

            /* Update catalogue entry for index. */
            this.updateState(IndexState.CLEAN, config.copy(marks = newMarks))
            LOGGER.debug("Rebuilding VAF index {} completed!", this@VAFIndex.name)
        }

        /**
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.xodusTx)
        }

        /**
         * Clears the [VAFIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            /* Truncate and replace store.*/
            this@VAFIndex.catalogue.environment.truncateStore(this@VAFIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@VAFIndex.catalogue.environment.openStore(this@VAFIndex.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@VAFIndex.name} is missing.")

            /* Update catalogue entry for index. */
            this.updateState(IndexState.STALE)
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
                if (value[i].value.toDouble() < this.marks!!.minimum[i] || value[i].value.toDouble() > this.marks!!.maximum[i]) {
                    this.dataStore.put(this.context.xodusTx, operation.tupleId.toKey(), VAFSignature.Binding.valueToEntry(VAFSignature.invalid(value.logicalSize)))
                    return false
                }
            }
            return this.dataStore.add(this.context.xodusTx, operation.tupleId.toKey(), VAFSignature.Binding.valueToEntry(this.marks!!.getSignature(value)))
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.UpdateOperation] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel]: UPDATES can be applied, if updated vector lies within the grid obtained upon creation of the index.
         *
         * @param operation The [Operation.DataManagementOperation.UpdateOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(operation: Operation.DataManagementOperation.UpdateOperation): Boolean {
            val value = operation.updates[this@VAFIndex.column]?.second
            require(value is RealVectorValue<*>) { "Only real vector values can be stored in a VAFIndex. This is a programmer's error!" }
            for (i in value.indices) {
                if (value[i].value.toDouble() < this.marks!!.minimum[i] || value[i].value.toDouble() >  this.marks!!.maximum[i]) {
                    this.dataStore.put(this.context.xodusTx, operation.tupleId.toKey(), VAFSignature.Binding.valueToEntry(VAFSignature.invalid(value.logicalSize)))
                    return false
                }
            }
            return this.dataStore.put(this.context.xodusTx, operation.tupleId.toKey(), VAFSignature.Binding.valueToEntry(this.marks!!.getSignature(value)))
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation.DeleteOperation] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method
         * implements the [VAFIndex]'es [WriteModel]: DELETES can always be applied.
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

            /** The [Bounds] objects used for filtering. */
            private val bounds: Bounds

            /** Internal [EntityTx] used to access actual values. */
            private val entityTx = this@Tx.context.getTx(this@VAFIndex.parent) as EntityTx

            /** The [HeapSelection] use for finding the top k entries. */
            private var selection = HeapSelection(this.predicate.k.toLong(), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))

            /** Cached in-memory version of the [VAFMarks] used by this [Cursor]. */
            private val marks = this@Tx.marks ?: throw IllegalStateException("VAFMarks could not be obtained. This is a programmer's error!")

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
                /* Initialize cursor. */
                val subTx = this@Tx.context.xodusTx.readonlySnapshot
                val cursor = this@Tx.dataStore.openCursor(subTx)
                val end = this.range.last.toKey()
                cursor.getSearchKey(this.range.first.toKey())

                /* Calculate a few values for future reference. */
                val columns = this@Tx.columns
                val produces = this@VAFIndex.produces(predicate).toTypedArray()
                var threshold = Double.MAX_VALUE
                while (cursor.next && cursor.key < end) {
                    val signature = VAFSignature.Binding.entryToValue(cursor.value)
                    if (this.selection.added < this.predicate.k || this.bounds.isVASSACandidate(signature, threshold)) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        val value = this.entityTx.read(tupleId, columns)[columns[0]] as VectorValue<*>
                        val distance = this.predicate.distance(this.query, value)!!
                        threshold = min(threshold, distance.value)
                        this.selection.offer(StandaloneRecord(tupleId, produces, arrayOf(distance, value)))
                    }
                }

                /* Log efficiency of VAF scan. */
                LOGGER.info("VAF scan: Skipped over ${(1.0 - this.selection.added.toDouble() / this@Tx.count()) * 100}% of entries.")

                /* Close Xodus cursor. */
                cursor.close()
                subTx.abort()
            }
        }
    }
}