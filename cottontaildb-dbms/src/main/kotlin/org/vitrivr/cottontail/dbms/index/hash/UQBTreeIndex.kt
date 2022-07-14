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
 * Represents an index in the Cottontail DB data model, that uses a persistent [HashMap] to map a
 * unique [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
class UQBTreeIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    companion object: IndexDescriptor<UQBTreeIndex> {
        /** [Logger] instance used by [BTreeIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(UQBTreeIndex::class.java)

        /** True since [UQBTreeIndex] supports incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [UQBTreeIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = false

        /** False, since [UQBTreeIndex] does not support partitioning. */
        override val supportsPartitioning: Boolean = false

        /**
         * Opens a [UQBTreeIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [UQBTreeIndex].
         * @param entity The [Entity] that holds the [UQBTreeIndex].
         * @return The opened [LuceneIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): UQBTreeIndex = UQBTreeIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [UQBTreeIndex].
         *
         * @param name The [Name.IndexName] of the [UQBTreeIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val store = (catalogue as DefaultCatalogue).environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize BTREE index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [UQBTreeIndex].
         *
         * @param name The [Name.IndexName] of the [UQBTreeIndex].
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
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<UQBTreeIndex> = UQBTreeIndexConfig

        /**
         * Returns the [UQBTreeIndexConfig]
         */
        override fun configBinding(): ComparableBinding = UQBTreeIndexConfig
    }

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.BTREE_UQ

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context [TransactionContext] to open the [Tx] for.
     * @return [Tx]
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Opens and returns a new [UQBTreeIndexRebuilder] object that can be used to rebuild with this [UQBTreeIndex].
     *
     * @param context [TransactionContext] to open the [UQBTreeIndexRebuilder] for.
     * @return [UQBTreeIndexRebuilder]
     */
    override fun newRebuilder(context: TransactionContext) = UQBTreeIndexRebuilder(this, context)

    /**
     * Since [UQBTreeIndex] does not support asynchronous re-indexing, this method will throw an error.
     */
    override fun newAsyncRebuilder(): AsyncIndexRebuilder<UQBTreeIndex>
        = throw UnsupportedOperationException("BTreeIndex does not support asynchronous index rebuilding.")

    /**
     * An [IndexTx] that affects this [UQBTreeIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The internal [XodusBinding] reference used for de-/serialization. */
        @Suppress("UNCHECKED_CAST")
        private val binding: XodusBinding<Value> = ValueSerializerFactory.xodus(this.columns[0].type, this.columns[0].nullable) as XodusBinding<Value>

        /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
        private var dataStore: Store = this@UQBTreeIndex.catalogue.environment.openStore(this@UQBTreeIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@UQBTreeIndex.name} is missing.")

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(key: Value, tupleId: TupleId) {
            val keyRaw = this.binding.valueToEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            if (!this.dataStore.add(this.context.xodusTx, keyRaw, tupleIdRaw)) {
                throw DatabaseException.ValidationException("Mapping of $key to tuple $tupleId could be added to UniqueHashIndex, because value must be unique.")
            }
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value): Boolean {
            val keyRaw = this.binding.valueToEntry(key)
            return this.dataStore.delete(this.context.xodusTx, keyRaw)
        }

        /**
         * Checks if the provided [Predicate] can be processed by this instance of [UQBTreeIndex]. [UQBTreeIndex] can be used to process IN and EQUALS
         * comparison operations on the specified column
         *
         * @param predicate The [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Atomic
                && !predicate.not
                && predicate.columns.contains(this.columns[0])
                && (predicate.operator is ComparisonOperator.In || predicate.operator is ComparisonOperator.Binary.Equal)

        /**
         * Returns a [List] of the [ColumnDef] produced by this [UQBTreeIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "Unique BTree index can only process boolean predicates." }
            this.columns.toList()
        }

        /**
         * The [UQBTreeIndex] does not return results in a particular order.
         *
         * @param predicate [Predicate] to check.
         * @return List that describes the sort order of the values returned by the [BTreeIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "Unique BTree index can only process boolean predicates." }
            emptyMap()
        }

        /**
         * Calculates the cost estimate of this [UQBTreeIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = this.txLatch.withLock {
            if (predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not) return Cost.INVALID
            when (val operator = predicate.operator) {
                is ComparisonOperator.Binary.Equal -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
                is ComparisonOperator.In -> (Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS) * operator.right.size + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
                else -> Cost.INVALID
            }
        }

        /**
         * Updates the [UQBTreeIndex] with the provided [DataEvent.Insert]
         *
         * @param event [DataEvent.Insert]s to process.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean {
            val value = event.data[this.columns[0]] ?: return true
            this.addMapping(value, event.tupleId)
            return true
        }

        /**
         * Updates the [UQBTreeIndex] with the provided [DataEvent.Update]s.
         *
         * @param event [DataEvent.Update]s to process.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val oldValue = event.data[this.columns[0]]?.first
            val newValue = event.data[this.columns[0]]?.second
            val removed = (oldValue == null || this.removeMapping(oldValue))
            if (newValue != null) this.addMapping(newValue, event.tupleId)
            return removed
        }

        /**
         * Updates the [UQBTreeIndex] with the provided [DataEvent.Delete]s.
         *
         * @param event [DataEvent.Delete]s to apply.
         */
        override fun tryApply(event: DataEvent.Delete) : Boolean  {
            val oldValue = event.data[this.columns[0]]
            return (oldValue == null) || this.removeMapping(oldValue)
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
         * Performs a lookup through this [UQBTreeIndex.Tx] and returns a [Cursor] of all [Record]s that match the [Predicate].
         * Only supports [BooleanPredicate.Atomic]s.
         *
         * The [Cursor] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Cursor]
         */
        override fun filter(predicate: Predicate)  = this.txLatch.withLock {
            object : Cursor<Record> {

                /** Local [BooleanPredicate.Atomic] instance. */
                private val predicate: BooleanPredicate.Atomic

                /** A [Queue] with values that should be queried. */
                private val queryValueQueue: Queue<Value> = LinkedList()

                /** Internal cursor used for navigation. */
                private val subTransaction = this@Tx.context.xodusTx.readonlySnapshot

                /** Internal cursor used for navigation. */
                private var cursor: jetbrains.exodus.env.Cursor

                /* Perform initial sanity checks. */
                init {
                    require(predicate is BooleanPredicate.Atomic) { "UQBTreeIndex.filter() does only support Atomic.Literal boolean predicates." }
                    require(!predicate.not) { "UniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                    this.predicate = predicate
                    when (predicate.operator) {
                        is ComparisonOperator.In -> this.queryValueQueue.addAll((predicate.operator as ComparisonOperator.In).right.mapNotNull { it.value })
                        is ComparisonOperator.Binary.Equal -> this.queryValueQueue.add((predicate.operator as ComparisonOperator.Binary.Equal).right.value ?: throw IllegalArgumentException("UQBTreeIndex.filter() does not support NULL operands."))
                        else -> throw IllegalArgumentException("UQBTreeIndex.filter() does only support EQUAL, IN or LIKE operators.")
                    }

                    /** Initialize cursor. */
                    this.cursor = this@Tx.dataStore.openCursor(this.subTransaction)
                }

                override fun moveNext(): Boolean {
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
         * The [UQBTreeIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Cursor].
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }
    }
}
