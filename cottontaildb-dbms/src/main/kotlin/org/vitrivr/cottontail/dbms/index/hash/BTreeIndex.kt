package org.vitrivr.cottontail.dbms.index.hash

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
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.xodus.XodusBinding
import java.util.*
import kotlin.concurrent.withLock

/**
 * Represents an [AbstractIndex] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.1.0
 */
class BTreeIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [BTreeIndex].
     */
    companion object: IndexDescriptor<BTreeIndex> {
        /** [Logger] instance used by [BTreeIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(BTreeIndex::class.java)

        /** True since [BTreeIndex] supports incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [BTreeIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = false

        /** False, since [BTreeIndex] does not support partitioning. */
        override val supportsPartitioning: Boolean = false

        /**
         * Opens a [BTreeIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [BTreeIndex].
         * @param entity The [Entity] that holds the [BTreeIndex].
         * @return The opened [LuceneIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): BTreeIndex = BTreeIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [BTreeIndex].
         *
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val store = (catalogue as DefaultCatalogue).environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize BTREE index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [BTreeIndex].
         *
         * @param name The [Name.IndexName] of the [BTreeIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            (catalogue as DefaultCatalogue).environment.removeStore(name.storeName(), context.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize BTREE index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns an empty [IndexConfig].
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<BTreeIndex> = object : IndexConfig<BTreeIndex> {}

        /**
         * Returns the [BTreeIndexConfig]
         */
        override fun configBinding(): ComparableBinding = BTreeIndexConfig
    }

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.BTREE

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [BTreeIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for.
     * @return [Tx]
     */
    override fun newTx(context: TransactionContext) = Tx(context)

    /**
     * Opens and returns a new [BTreeIndexRebuilder] object that can be used to rebuild with this [BTreeIndex].
     *
     * @param context If the [TransactionContext] to create the [BTreeIndexRebuilder] for.
     * @return [BTreeIndexRebuilder]
     */
    override fun newRebuilder(context: TransactionContext) = BTreeIndexRebuilder(this, context)

    /**
     * Since [BTreeIndex] does not support asynchronous re-indexing, this method will throw an error.
     */
    override fun newAsyncRebuilder(): AsyncIndexRebuilder<BTreeIndex>
        = throw UnsupportedOperationException("BTreeIndex does not support asynchronous index rebuilding.")

    /**
     * An [IndexTx] that affects this [BTreeIndex].
     */
    inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The internal [XodusBinding] reference used for de-/serialization. */
        @Suppress("UNCHECKED_CAST")
        private val binding: XodusBinding<Value> = ValueSerializerFactory.xodus(this.columns[0].type, this.columns[0].nullable) as XodusBinding<Value>

        /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
        private val dataStore: Store = this@BTreeIndex.catalogue.environment.openStore(this@BTreeIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@BTreeIndex.name} is missing.")

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
         * Returns a [List] of the [ColumnDef] produced by this [BTreeIndex.Tx].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "BTree index can only process boolean predicates." }
            return this.columns.toList()
        }

        /**
         * The [BTreeIndex] does not return results in a particular order.
         *
         * @param predicate [Predicate] to check.
         * @return List that describes the sort order of the values returned by the [BTreeIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "BTree index can only process boolean predicates." }
            emptyMap()
        }

        /**
         * Calculates the cost estimate of this [BTreeIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = this.txLatch.withLock {
            if (predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not) return Cost.INVALID
            return when (val operator = predicate.operator) {
                is ComparisonOperator.Binary.Equal -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
                is ComparisonOperator.Binary.Like -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
                is ComparisonOperator.In -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat()) * operator.right.size
                else -> Cost.INVALID
            }
        }

        /**
         * Updates the [BTreeIndex] with the provided [DataEvent.Insert].
         *
         * @param event [DataEvent.Insert] to apply.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean {
            val value = event.data[this.columns[0]] ?: return true
            return this.addMapping(value, event.tupleId)
        }

        /**
         * Updates the [BTreeIndex] with the provided [DataEvent.Update].
         *
         * @param event [DataEvent.Update] to apply.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val old = event.data[this.columns[0]]?.first
            val new = event.data[this.columns[0]]?.second
            val removed = (old == null || this.removeMapping(old, event.tupleId))
            val added = (new == null || this.addMapping(new, event.tupleId))
            return removed && added
        }

        /**
         * Updates the [BTreeIndex] with the provided [DataEvent.Delete].
         *
         * @param event [DataEvent.Delete] to apply.
         */
        override fun tryApply(event: DataEvent.Delete): Boolean {
            val old = event.data[this.columns[0]] ?: return true
            return this.removeMapping(old, event.tupleId)
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
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Cursor].
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> {
            throw UnsupportedOperationException("The NonUniqueHashIndex does not support ranged filtering!")
        }
    }
}
