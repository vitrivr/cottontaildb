package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import kotlin.concurrent.withLock
import kotlin.math.log10

/**
 * Represents an [AbstractIndex] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.2.0
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
         * @param context The [Transaction] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: Transaction): Boolean = try {
            val store = catalogue.transactionManager.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, context.xodusTx, true)
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
         * @param context The [Transaction] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: Transaction): Boolean = try {
            catalogue.transactionManager.environment.removeStore(name.storeName(), context.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize BTREE index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns an empty [IndexConfig].
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<BTreeIndex> = BTreeIndexConfig

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
     * @param context If the [Transaction] to create the [IndexTx] for.
     * @return [Tx]
     */
    override fun newTx(context: QueryContext)
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * Opens and returns a new [QueryContext] object that can be used to rebuild with this [BTreeIndex].
     *
     * @param context If the [Transaction] to create the [BTreeIndexRebuilder] for.
     * @return [QueryContext]
     */
    override fun newRebuilder(context: QueryContext) = BTreeIndexRebuilder(this, context)

    /**
     * Since [BTreeIndex] does not support asynchronous re-indexing, this method will throw an error.
     */
    override fun newAsyncRebuilder(context: QueryContext): AsyncIndexRebuilder<BTreeIndex>
        = throw UnsupportedOperationException("BTreeIndex does not support asynchronous index rebuilding.")

    /**
     * An [IndexTx] that affects this [BTreeIndex].
     */
    inner class Tx(context: QueryContext) : AbstractIndex.Tx(context) {

        /** The internal [ValueSerializer] reference used for de-/serialization. */
        @Suppress("UNCHECKED_CAST")
        internal val binding: ValueSerializer<Value> = SerializerFactory.value(this.columns[0].type) as ValueSerializer<Value>

        /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
        internal val dataStore: Store = this@BTreeIndex.catalogue.transactionManager.environment.openStore(this@BTreeIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.txn.xodusTx, false)
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
            val keyRaw = this.binding.toEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return this.dataStore.put(this.context.txn.xodusTx, keyRaw, tupleIdRaw)
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = this.binding.toEntry(key)
            val valueRaw = LongBinding.longToCompressedEntry(tupleId)
            val cursor = this.dataStore.openCursor(this.context.txn.xodusTx)
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
        override fun canProcess(predicate: Predicate) = predicate is BooleanPredicate.Comparison
                && predicate.columns.all { it.physical == this.columns[0] }
                && (predicate.operator is ComparisonOperator.In || predicate.operator is ComparisonOperator.Equal || predicate.operator is ComparisonOperator.Like)

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
            mapOf(NotPartitionableTrait to NotPartitionableTrait)
        }

        /**
         * Calculates the cost estimate of this [BTreeIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = this.txLatch.withLock {
            if (predicate !is BooleanPredicate.Comparison || predicate.columns.any { it.physical != this.columns[0] }) return Cost.INVALID
            val entityTx = this.dbo.parent.newTx(this.context)
            val statistics = this.columns.associateWith { entityTx.columnForName(it.name).newTx(this.context).statistics() }
            val selectivity = with(this@Tx.context.bindings) {
                with(MissingTuple) {
                    NaiveSelectivityCalculator.estimate(predicate, statistics)
                }
            }
            val count = this.count()                /* Number of entries. */
            val countOut = selectivity(count)       /* Number of entries actually selected (comparison still required). */
            val search = log10(count.toFloat())     /* Overhead for search into the index. */
            return when (predicate.operator) {
                is ComparisonOperator.Equal,
                is ComparisonOperator.NotEqual,
                is ComparisonOperator.Greater,
                is ComparisonOperator.Less,
                is ComparisonOperator.GreaterEqual,
                is ComparisonOperator.LessEqual,
                is ComparisonOperator.In -> Cost.DISK_ACCESS_READ_SEQUENTIAL * (search + countOut) + predicate.cost * countOut
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
            this.dataStore.count(this.context.txn.xodusTx)
        }

        /**
         * Performs a lookup through this [BTreeIndex.Tx] and returns a [Iterator] of all [Tuple]s that match the [Predicate].
         * Only supports [BooleanPredicate.Comparison]s.
         *
         * The [Cursor] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Cursor]
         */
        override fun filter(predicate: Predicate) = this.txLatch.withLock {
            require(predicate is BooleanPredicate.Comparison) { "BTreeIndex.filter() does only support BooleanPredicate.Atomic boolean predicates." }
            with(this.context.bindings) {
                with(MissingTuple) {
                    when(val op = predicate.operator) {
                        is ComparisonOperator.Equal -> BTreeIndexCursor.Equals(op.right.getValue()!!, this@Tx)
                        is ComparisonOperator.Greater -> BTreeIndexCursor.Greater(op.right.getValue()!!, this@Tx)
                        is ComparisonOperator.GreaterEqual -> BTreeIndexCursor.GreaterEqual(op.right.getValue()!!, this@Tx)
                        is ComparisonOperator.Less -> BTreeIndexCursor.Less(op.right.getValue()!!, this@Tx)
                        is ComparisonOperator.LessEqual -> BTreeIndexCursor.LessEqual(op.right.getValue()!!, this@Tx)
                        is ComparisonOperator.In -> BTreeIndexCursor.In(op.right.getValues(), this@Tx)
                        else -> throw IllegalArgumentException("BTreeIndex.filter() does only support =,>=,<=,>,< and IN operators.")
                    }
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
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
            throw UnsupportedOperationException("BTreeIndex does not support ranged filtering!")
        }
    }
}
