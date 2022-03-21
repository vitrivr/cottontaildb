package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
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
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.xodus.XodusBinding
import java.util.*
import kotlin.concurrent.withLock

/**
 * Represents an [AbstractIndex] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.0.0
 */
class BTreeIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [BTreeIndex].
     */
    companion object: IndexDescriptor<BTreeIndex> {
        /**
         * Opens a [BTreeIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [BTreeIndex].
         * @param entity The [DefaultEntity] that holds the [BTreeIndex].
         * @return The opened [LuceneIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity): BTreeIndex = BTreeIndex(name, entity)

        /**
         * Tries to initialize the [Store] for a [BTreeIndex].
         *
         * @param name The [Name.IndexName] of the [BTreeIndex].
         * @param entity The [DefaultEntity] that holds the [BTreeIndex].
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean {
            val store = entity.dbo.catalogue.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, entity.context.xodusTx, true)
            return store != null
        }

        /**
         * Generates and returns an empty [IndexConfig].
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<BTreeIndex> = object : IndexConfig<BTreeIndex>{}

        /**
         * Returns the [BTreeIndexConfig]
         */
        override fun configBinding(): ComparableBinding = BTreeIndexConfig
    }

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.BTREE

    /** True since [BTreeIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [BTreeIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The dummy [BTreeIndexConfig]. */
    override val config: IndexConfig<BTreeIndex> = BTreeIndexConfig

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [BTreeIndex].
     *
     * [BTreeIndex] can be used to process EQUALS, IN AND LIKE comparison operations on the specified column
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
     * Returns a [List] of the [ColumnDef] produced by this [BTreeIndex].
     *
     * @return [List] of [ColumnDef].
     */
    override fun produces(predicate: Predicate): List<ColumnDef<*>> {
        require(predicate is BooleanPredicate) { "BTree index can only process boolean predicates." }
        return this.columns.toList()
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [BTreeIndex]
     */
    override fun close() {
        /* No op. */
    }

    /**
     * An [IndexTx] that affects this [BTreeIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The internal [XodusBinding] reference used for de-/serialization. */
        @Suppress("UNCHECKED_CAST")
        private val binding: XodusBinding<Value> = ValueSerializerFactory.xodus(this.columns[0].type, this.columns[0].nullable) as XodusBinding<Value>

        /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
        private var dataStore: Store = this@BTreeIndex.catalogue.environment.openStore(this@BTreeIndex.name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@BTreeIndex.name} is missing.")

        /** The dummy [BTreeIndexConfig]. */
        override val config: IndexConfig<BTreeIndex>
            get() = this@BTreeIndex.config

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = this.binding.valueToEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return this.dataStore.put(this.context.xodusTx, keyRaw, tupleIdRaw)
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = this.binding.valueToEntry(key)
            val valueRaw = LongBinding.longToCompressedEntry(tupleId)
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            val ret = cursor.getSearchBoth(keyRaw, valueRaw) && cursor.deleteCurrent()
            cursor.close()
            return ret
        }

        /**
         * Calculates the cost estimate of this [BTreeIndex.Tx] processing the provided [Predicate].
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
         * (Re-)builds the [BTreeIndex].
         */
        override fun rebuild() = this.txLatch.withLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Truncate and reopen old store. */
            this.clear()

            /* Iterate over entity and update index with entries. */
            val cursor = entityTx.cursor(this.columns)
            cursor.forEach { record ->
                val value = record[this.columns[0]] ?: throw TxException.TxValidationException(
                    this.context.txId,
                    "A value cannot be null for instances of NonUniqueHashIndex ${this@BTreeIndex.name} but given value is (value = null, tupleId = ${record.tupleId})."
                )
                this.addMapping(value, record.tupleId)
            }

            /* Close cursor. */
            cursor.close()
        }

        /**
         * Updates the [BTreeIndex] with the provided [Operation.DataManagementOperation.InsertOperation].
         *
         * @param operation [Operation.DataManagementOperation.InsertOperation] to apply.
         */
        override fun insert(operation: Operation.DataManagementOperation.InsertOperation) = this.txLatch.withLock {
            val value = operation.inserts[this.dbo.columns[0]]
            if (value != null) {
                this.addMapping(value, operation.tupleId)
            }
        }

        /**
         * Updates the [BTreeIndex] with the provided [Operation.DataManagementOperation.UpdateOperation].
         *
         * @param operation [Operation.DataManagementOperation.UpdateOperation] to apply.
         */
        override fun update(operation: Operation.DataManagementOperation.UpdateOperation) = this.txLatch.withLock {
            val old = operation.updates[this.dbo.columns[0]]?.first
            if (old != null) {
                this.removeMapping(old, operation.tupleId)
            }
            val new = operation.updates[this.dbo.columns[0]]?.second
            if (new != null) {
                this.addMapping(new, operation.tupleId)
            }
        }

        /**
         * Updates the [BTreeIndex] with the provided [Operation.DataManagementOperation.DeleteOperation].
         *
         * @param operation [Operation.DataManagementOperation.DeleteOperation] to apply.
         */
        override fun delete(operation: Operation.DataManagementOperation.DeleteOperation) = this.txLatch.withLock {
            val old = operation.deleted[this.dbo.columns[0]]
            if (old != null) {
                this.removeMapping(old, operation.tupleId)
            }
        }

        /**
         * Returns the number of entries in this [UQBTreeIndex].
         *
         * @return Number of entries in this [UQBTreeIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.xodusTx)
        }

        /**
         * Clears the [BTreeIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            this@BTreeIndex.parent.parent.parent.environment.truncateStore(this@BTreeIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@BTreeIndex.parent.parent.parent.environment.openStore(
                this@BTreeIndex.name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false
            ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@BTreeIndex.name} is missing.")
        }

        /**
         * Performs a lookup through this [BTreeIndex.Tx] and returns a [Iterator] of all [Record]s that match the [Predicate].
         * Only supports [BooleanPredicate.Atomic]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = this.txLatch.withLock {
            object : Cursor<Record> {
                /** A [Queue] with values that should be queried. */
                private val queryValueQueue: Queue<Value> = LinkedList()

                /** Internal cursor used for navigation. */
                private val subTransaction = this@Tx.context.xodusTx.readonlySnapshot

                /** Internal cursor used for navigation. */
                private val cursor: jetbrains.exodus.env.Cursor

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
                    this.cursor = this@Tx.dataStore.openCursor(this.subTransaction)
                }

                override fun moveNext(): Boolean {
                    /* Check for an existing duplicate for the current cursor. */
                    try {
                        if (this.cursor.nextDup) return true
                    } catch (e: IllegalStateException) {
                        /* Note: Cursors has not been initialized; this is the case for the first call OR when getSearchKey doesn't return a result. */
                    }

                    /* Now update cursor and check again. */
                    var nextQueryValue = this.queryValueQueue.poll()
                    while (nextQueryValue != null) {
                        if (this.cursor.getSearchKey(this@Tx.binding.valueToEntry(nextQueryValue)) != null) {
                            return true
                        }
                        nextQueryValue = this.queryValueQueue.poll()
                    }
                    return false
                }

                override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)

                override fun value(): Record = StandaloneRecord(this.key(), this@Tx.columns, arrayOf(this@Tx.binding.entryToValue(this.cursor.key)))

                override fun close() {
                    this.cursor.close()
                    this.subTransaction.abort()
                }
            }
        }

        /**
         * The [BTreeIndex] does not support ranged filtering!
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
