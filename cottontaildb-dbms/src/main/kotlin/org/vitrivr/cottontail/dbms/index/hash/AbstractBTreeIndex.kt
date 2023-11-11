package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.util.*
import kotlin.concurrent.withLock
import kotlin.math.log10

/**
 * RA [DefaultIndex] in the Cottontail DB data model, that uses a persistent BTree data structure for indexing.
 *
 * Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto
 * @author Ralph Gasser
 * @version 4.0.0
 */
abstract class AbstractBTreeIndex(name: Name.IndexName, parent: DefaultEntity) : DefaultIndex(name, parent) {
    /**
     * Since [BTreeIndex] does not support asynchronous re-indexing, this method will throw an error.
     */
    final override fun newAsyncRebuilder(context: QueryContext): AsyncIndexRebuilder<BTreeIndex> = throw UnsupportedOperationException("BTreeIndex does not support asynchronous index rebuilding.")

    /**
     * An [IndexTx] that affects this [BTreeIndex].
     */
    abstract inner class Tx(parent: DefaultEntity.Tx) : DefaultIndex.Tx(parent) {

        /** The internal [ValueSerializer] reference used for de-/serialization. */
        @Suppress("UNCHECKED_CAST")
        private val binding: ValueSerializer<Value> = SerializerFactory.value(this.columns[0].type) as ValueSerializer<Value>

        /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
        protected abstract var store: Store

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        internal fun addMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = this.binding.toEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return this.store.put(this.xodusTx, keyRaw, tupleIdRaw)
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
            val cursor = this.store.openCursor(this.xodusTx)
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
        final override fun canProcess(predicate: Predicate): Boolean {
            if (predicate !is BooleanPredicate.Comparison) return false
            if (!predicate.columns.contains(this.columns[0])) return false
            return when (predicate.operator) {
                is ComparisonOperator.Equal,
                is ComparisonOperator.Greater,
                is ComparisonOperator.Less,
                is ComparisonOperator.GreaterEqual,
                is ComparisonOperator.LessEqual,
                is ComparisonOperator.In,
                is ComparisonOperator.Like -> true
                else -> false
            }
        }

        /**
         * Returns a [List] of the [ColumnDef] produced by this [BTreeIndex.Tx].
         *
         * @return [List] of [ColumnDef].
         */
        final override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "BTree index can only process boolean predicates." }
            return this.columns.toList()
        }

        /**
         * The [BTreeIndex] does not return results in a particular order.
         *
         * @param predicate [Predicate] to check.
         * @return List that describes the sort order of the values returned by the [BTreeIndex]
         */
        final override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "BTree index can only process boolean predicates." }
            mapOf(NotPartitionableTrait to NotPartitionableTrait)
        }

        /**
         * Calculates the cost estimate of this [BTreeIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        context(BindingContext)
        final override fun costFor(predicate: Predicate): Cost = this.txLatch.withLock {
            if (predicate !is BooleanPredicate.Comparison || predicate.columns.first() != this.columns[0]) return Cost.INVALID
            val statistics = this.columns.associateWith {
                this.transaction.manager.statistics[it.name]?.statistics ?: it.type.defaultStatistics<Value>()
            }

            /* Generate selectivity estimate. */
            val selectivity = with(this@BindingContext) {
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
        final override fun tryApply(event: DataEvent.Insert): Boolean {
            val value = event.data[this.columns[0]] ?: return true
            return this.addMapping(value, event.tupleId)
        }

        /**
         * Updates the [BTreeIndex] with the provided [DataEvent.Update].
         *
         * @param event [DataEvent.Update] to apply.
         */
        final override fun tryApply(event: DataEvent.Update): Boolean {
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
        final override fun tryApply(event: DataEvent.Delete): Boolean {
            val old = event.data[this.columns[0]] ?: return true
            return this.removeMapping(old, event.tupleId)
        }

        /**
         * Drops (i.e., removes) the [Index] backed by this [IndexTx].
         */
        final override fun drop() = this.txLatch.withLock {
            /* Delete metadata entry. */
            val indexMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.INDEX_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")
            if (!indexMetadataStore.delete(this.xodusTx, NameBinding.Index.toEntry(this@AbstractBTreeIndex.name))) {
                throw DatabaseException.DataCorruptionException("DROP INDEX $name failed: Failed to delete catalogue entry.")
            }

            /* Remove store. */
            this.xodusTx.environment.removeStore(this@AbstractBTreeIndex.name.storeName(), this.xodusTx)
        }

        /**
         * Returns the number of entries in this [UQBTreeIndex].
         *
         * @return Number of entries in this [UQBTreeIndex]
         */
        final override fun count(): Long = this.txLatch.withLock {
            this.store.count(this.xodusTx)
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
        context(BindingContext)
        final override fun filter(predicate: Predicate) = this.txLatch.withLock {
            require(predicate is BooleanPredicate.Comparison) { "BTreeIndex.filter() does only support BooleanPredicate.Atomic boolean predicates." }
            with(this@BindingContext) {
                with(MissingTuple) {
                    when(val op = predicate.operator) {
                        is ComparisonOperator.Equal -> BTreeIndexCursor.Equals(this@Tx, op.right.getValue() ?: throw IllegalArgumentException("BTreeIndex.filter() does only support non-null values."))
                        is ComparisonOperator.Greater -> BTreeIndexCursor.Greater(this@Tx, op.right.getValue() ?: throw IllegalArgumentException("BTreeIndex.filter() does only support non-null values."))
                        is ComparisonOperator.GreaterEqual -> BTreeIndexCursor.GreaterEqual(this@Tx, op.right.getValue() ?: throw IllegalArgumentException("BTreeIndex.filter() does only support non-null values."))
                        is ComparisonOperator.Less -> BTreeIndexCursor.Less(this@Tx, op.right.getValue() ?: throw IllegalArgumentException("BTreeIndex.filter() does only support non-null values."))
                        is ComparisonOperator.LessEqual -> BTreeIndexCursor.LessEqual(this@Tx, op.right.getValue() ?: throw IllegalArgumentException("BTreeIndex.filter() does only support non-null values."))
                        is ComparisonOperator.In -> BTreeIndexCursor.In(this@Tx, LinkedList(op.right.getValues().filterNotNull()))
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
        final override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
            throw UnsupportedOperationException("BTreeIndex does not support ranged filtering!")
        }
    }
}
