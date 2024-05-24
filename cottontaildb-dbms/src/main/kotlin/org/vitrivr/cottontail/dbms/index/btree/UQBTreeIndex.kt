package org.vitrivr.cottontail.dbms.index.btree

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
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
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.AsyncPQIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import org.vitrivr.cottontail.server.Instance
import org.vitrivr.cottontail.storage.serializers.ValueBinding
import kotlin.math.log10

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
         * @param parent The [EntityTx] that requested index initialization.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, parent: EntityTx): Boolean = try {
            require(parent is DefaultEntity.Tx) { "A UQBTreeIndex can only be used with a DefaultEntity.Tx!" }
            parent.xodusTx.environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, parent.xodusTx, true) != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize BTREE index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [UQBTreeIndex].
         *
         * @param name The [Name.IndexName] of the [UQBTreeIndex].
         * @param parent The [EntityTx] that requested index de-initialization.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, parent: EntityTx): Boolean = try {
            require(parent is DefaultEntity.Tx) { "A UQBTreeIndex can only be used with a DefaultEntity.Tx!" }
            parent.xodusTx.environment.removeStore(name.storeName(), parent.xodusTx)
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
     * Opens and returns a new [IndexTx] object that can be used to interact with this [UQBTreeIndex].
     *
     * @param parent If parent [EntityTx] this [IndexTx] belongs to.
     */
    override fun newTx(parent: EntityTx): IndexTx {
        require(parent is DefaultEntity.Tx) { "UQBTreeIndex can only be used with DefaultEntity.Tx" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * Opens and returns a new [UQBTreeIndexRebuilder] object that can be used to rebuild with this [UQBTreeIndex].
     *
     * @param context [QueryContext] to open the [UQBTreeIndexRebuilder] for.
     * @return [UQBTreeIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext) = UQBTreeIndexRebuilder(this, context)

    /**
     * Opens and returns a new [AsyncPQIndexRebuilder] object that can be used to rebuild with this [UQBTreeIndex].
     *
     * @param instance The [Instance] that requested the [AsyncIndexRebuilder].
     * @return [AsyncPQIndexRebuilder]
     */
    override fun newAsyncRebuilder(instance: Instance): AsyncIndexRebuilder<*> {
        throw UnsupportedOperationException("BTreeIndex does not support asynchronous rebuilding.")
    }

    /**
     * An [IndexTx] that affects this [UQBTreeIndex].
     */
    inner class Tx(parent: DefaultEntity.Tx) : AbstractIndex.Tx(parent) {

        /** The internal [ValueBinding] reference used for de-/serialization. */
        @Suppress("UNCHECKED_CAST")
        private val binding: ValueBinding<Value> = ValueBinding(ValueSerializer.serializer(this.columns[0].type)) as ValueBinding<Value>

        /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
        private val dataStore: Store = this.xodusTx.environment.openStore(this@UQBTreeIndex.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx, false)
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
            val keyRaw = this.binding.toEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            if (!this.dataStore.add(this.xodusTx, keyRaw, tupleIdRaw)) {
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
            val keyRaw = this.binding.toEntry(key)
            return this.dataStore.delete( this.xodusTx, keyRaw)
        }

        /**
         * Checks if the provided [Predicate] can be processed by this instance of [UQBTreeIndex]. [UQBTreeIndex] can be used to process IN and EQUALS
         * comparison operations on the specified column
         *
         * @param predicate The [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Comparison
            && predicate.columns.all { it.physical == this.columns[0] }
            && (predicate.operator is ComparisonOperator.In || predicate.operator is ComparisonOperator.Equal)

        /**
         * Returns a [List] of the [ColumnDef] produced by this [UQBTreeIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> {
            require(predicate is BooleanPredicate) { "Unique BTree index can only process boolean predicates." }
            return this.columns.toList()
        }

        /**
         * The [UQBTreeIndex] does not return results in a particular order.
         *
         * @param predicate [Predicate] to check.
         * @return List that describes the sort order of the values returned by the [BTreeIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> {
            require(predicate is BooleanPredicate) { "Unique BTree index can only process boolean predicates." }
            return mapOf(NotPartitionableTrait to NotPartitionableTrait)
        }

        /**
         * Calculates the cost estimate of this [UQBTreeIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        @Synchronized
        override fun costFor(predicate: Predicate): Cost {
            if (predicate !is BooleanPredicate.Comparison || predicate.columns.any { it.physical != this.columns[0] }) return Cost.INVALID
            val statistics = this.columns.associateWith { this.parent.statistics(it) }
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
         * Updates the [UQBTreeIndex] with the provided [DataEvent.Insert]
         *
         * @param event [DataEvent.Insert]s to process.
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Insert): Boolean {
            val value = event.tuple[this.columns[0].name] ?: return true
            this.addMapping(value, event.tuple.tupleId)
            return true
        }

        /**
         * Updates the [UQBTreeIndex] with the provided [DataEvent.Update]s.
         *
         * @param event [DataEvent.Update]s to process.
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Update): Boolean {
            val oldValue = event.oldTuple[this.columns[0].name]
            val newValue = event.newTuple[this.columns[0].name]
            val removed = (oldValue == null || this.removeMapping(oldValue))
            if (newValue != null) this.addMapping(newValue, event.newTuple.tupleId)
            return removed
        }

        /**
         * Updates the [UQBTreeIndex] with the provided [DataEvent.Delete]s.
         *
         * @param event [DataEvent.Delete]s to apply.
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Delete) : Boolean  {
            val oldValue = event.oldTuple[this.columns[0].name]
            return (oldValue == null) || this.removeMapping(oldValue)
        }

        /**
         * Returns the number of entries in this [UQBTreeIndex].
         *
         * @return Number of entries in this [UQBTreeIndex]
         */
        @Synchronized
        override fun count(): Long = this.dataStore.count(this.xodusTx)

        /**
         * Performs a lookup through this [UQBTreeIndex.Tx] and returns a [Cursor] of all [Tuple]s that match the [Predicate].
         * Only supports [BooleanPredicate.Comparison]s.
         *
         * The [Cursor] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Cursor]
         */
        @Synchronized
        override fun filter(predicate: Predicate): Cursor<Tuple> {
            require(predicate is BooleanPredicate.Comparison) { "BTreeIndex.filter() does only support BooleanPredicate.Atomic boolean predicates." }
            with(this.context.bindings) {
                with(MissingTuple) {
                    val columns = this@Tx.columnsFor(predicate).toTypedArray()
                    return when(val op = predicate.operator) {
                        is ComparisonOperator.Equal -> UQBTreeIndexCursor.Equals(op.right.getValue()!!, this@Tx, columns)
                        is ComparisonOperator.Greater -> UQBTreeIndexCursor.Greater(op.right.getValue()!!, this@Tx, columns)
                        is ComparisonOperator.GreaterEqual -> UQBTreeIndexCursor.GreaterEqual(op.right.getValue()!!, this@Tx, columns)
                        is ComparisonOperator.Less -> UQBTreeIndexCursor.Less(op.right.getValue()!!, this@Tx, columns)
                        is ComparisonOperator.LessEqual -> UQBTreeIndexCursor.LessEqual(op.right.getValue()!!, this@Tx, columns)
                        is ComparisonOperator.In -> UQBTreeIndexCursor.In(op.right.getValues(), this@Tx, columns)
                        else -> throw IllegalArgumentException("BTreeIndex.filter() does only support =,>=,<=,>,< and IN operators.")
                    }
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
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }
    }
}
