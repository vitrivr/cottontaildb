package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.storage.serializers.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding
import java.util.*
import kotlin.concurrent.withLock

/**
 * Represents an [AbstractIndex] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.0.0
 */
class NonUniqueHashIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.BTREE

    /** True since [NonUniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [NonUniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The [NonUniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** [UniqueHashIndex] does not have an [IndexConfig]*/
    override val config: IndexConfig = NoIndexConfig

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [NonUniqueHashIndex].
     *
     * [NonUniqueHashIndex] can be used to process EQUALS, IN AND LIKE comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean {
        if (predicate !is BooleanPredicate.Atomic) return false
        if (predicate.not) return false
        if (!predicate.columns.contains(this.columns[0])) return false
        return when (predicate.operator) {
            is ComparisonOperator.Binary.Equal,
            is ComparisonOperator.In -> true
            is ComparisonOperator.Binary.Like -> ((predicate.operator as ComparisonOperator.Binary.Like).right.value is LikePatternValue.StartsWith)
            else -> false
        }
    }

    /**
     * Calculates the cost estimate of this [NonUniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost {
        if (predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not) return Cost.INVALID
        return when (val operator = predicate.operator) {
            is ComparisonOperator.Binary.Equal -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
            is ComparisonOperator.Binary.Like -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
            is ComparisonOperator.In -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat()) * operator.right.size
            else -> Cost.INVALID
        }
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [NonUniqueHashIndex]
     */
    override fun close() {
        /* No op. */
    }

    /**
     * An [IndexTx] that affects this [NonUniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The internal [XodusBinding] reference used for de-/serialization. */
        private val binding: XodusBinding<*> = ValueSerializerFactory.xodus(this.columns[0].type, this.columns[0].nullable)

        /** [NonUniqueHashIndex] does not have an [IndexConfig]*/
        override val config: IndexConfig
            get() = this@NonUniqueHashIndex.config

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = (this.binding as XodusBinding<Value>).valueToEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return if (this.dataStore.exists(this.context.xodusTx, keyRaw, tupleIdRaw)) {
                this.dataStore.put(this.context.xodusTx, keyRaw, tupleIdRaw)
            } else {
                false
            }
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = (this.binding as XodusBinding<Value>).valueToEntry(key)
            val valueRaw = LongBinding.longToCompressedEntry(tupleId)
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            if (cursor.getSearchBoth(keyRaw, valueRaw)) {
                return cursor.deleteCurrent()
            } else {
                return false
            }
        }

        /**
         * (Re-)builds the [NonUniqueHashIndex].
         */
        override fun rebuild() = this.txLatch.withLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Truncate and reopen old store. */
            this.clear()
            entityTx.cursor(this.dbo.columns).forEach { record ->
                val value = record[this.dbo.columns[0]] ?: throw TxException.TxValidationException(
                    this.context.txId,
                    "A value cannot be null for instances of NonUniqueHashIndex ${this@NonUniqueHashIndex.name} but given value is (value = null, tupleId = ${record.tupleId})."
                )
                this.addMapping(value, record.tupleId)
            }
        }

        /**
         * Clears the [NonUniqueHashIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            this@NonUniqueHashIndex.parent.parent.parent.environment.truncateStore(this@NonUniqueHashIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@NonUniqueHashIndex.parent.parent.parent.environment.openStore(
                this@NonUniqueHashIndex.name.storeName(),
                StoreConfig.WITH_DUPLICATES_WITH_PREFIXING,
                this.context.xodusTx,
                false
            ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@NonUniqueHashIndex.name} is missing.")
        }

        /**
         * Updates the [NonUniqueHashIndex] with the provided [Record]. This method determines, whether
         * the [Record] affected by the [Operation.DataManagementOperation] should be added or updated
         *
         * @param event [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.txLatch.withLock {
            when (event) {
                is Operation.DataManagementOperation.InsertOperation -> {
                    val value = event.inserts[this.dbo.columns[0]]
                    if (value != null) {
                        this.addMapping(value, event.tupleId)
                    }
                }
                is Operation.DataManagementOperation.UpdateOperation -> {
                    val old = event.updates[this.dbo.columns[0]]?.first
                    if (old != null) {
                        this.removeMapping(old, event.tupleId)
                    }
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new != null) {
                        this.addMapping(new, event.tupleId)
                    }
                }
                is Operation.DataManagementOperation.DeleteOperation -> {
                    val old = event.deleted[this.dbo.columns[0]]
                    if (old != null) {
                        this.removeMapping(old, event.tupleId)
                    }
                }
            }
        }

        /**
         * Performs a lookup through this [NonUniqueHashIndex.Tx] and returns a [Iterator] of all [Record]s that match the [Predicate].
         * Only supports [BooleanPredicate.Atomic]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Cursor<Record> {
            /** A [Queue] with values that should be queried. */
            private val queryValueQueue: Queue<Value> = LinkedList()

            /** The current query [Value]. */
            private var queryValue: Value

            /** Internal cursor used for navigation. */
            private var cursor: jetbrains.exodus.env.Cursor

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic) { "NonUniqueHashIndex.filter() does only support Atomic.Literal boolean predicates." }
                require(!predicate.not) { "NonUniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                when (predicate.operator) {
                    is ComparisonOperator.In -> this.queryValueQueue.addAll((predicate.operator as ComparisonOperator.In).right.mapNotNull { it.value })
                    is ComparisonOperator.Binary.Equal -> this.queryValueQueue.add((predicate.operator as ComparisonOperator.Binary.Equal).right.value ?: throw IllegalArgumentException("UniqueHashIndex.filter() does not support NULL operands."))
                    else -> throw IllegalArgumentException("UniqueHashIndex.filter() does only support EQUAL and IN operators.")
                }

                /** Initialize cursor. */
                this.cursor = this@Tx.dataStore.openCursor(this@Tx.context.xodusTx)
                this.queryValue = this.queryValueQueue.poll() ?: throw IllegalArgumentException("UniqueHashIndex.filter() does not support NULL operands.")
                this.cursor.getSearchKey(StringBinding.BINDING.objectToEntry(this.queryValue))
            }

            override fun moveNext(): Boolean {
                if (this.cursor.nextDup) return true
                this.queryValue = this.queryValueQueue.poll() ?: return false
                return this.cursor.getSearchKey(StringBinding.BINDING.objectToEntry(this.queryValue)) != null
            }

            override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

            override fun value(): Record = StandaloneRecord(this.key(), this@NonUniqueHashIndex.produces, arrayOf(this@Tx.binding.entryToValue(this.cursor.value)))

            override fun close() = this.cursor.close()
        }

        /**
         * The [NonUniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The NonUniqueHashIndex does not support ranged filtering!")
        }
    }
}
