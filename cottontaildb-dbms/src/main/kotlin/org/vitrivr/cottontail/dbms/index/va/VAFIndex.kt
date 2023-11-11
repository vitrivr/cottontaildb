package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.LimitTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue.Companion.INDEX_STRUCT_STORE_NAME
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.va.rebuilder.AsyncVAFIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.rebuilder.VAFIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.values.RealVectorValueStatistics
import kotlin.concurrent.withLock

/**
 * An [DefaultIndex] structure for proximity based search (NNS / FNS) that uses a vector approximation (VA) file ([1]). Can be used for all types of [RealVectorValue]s
 * and all Minkowski metrics (L1, L2, Lp etc.).
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann
 * @author Ralph Gasser
 * @version 3.6.0
 */
class VAFIndex(name: Name.IndexName, parent: DefaultEntity) : DefaultIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [VAFIndex].
     */
    companion object: IndexDescriptor<VAFIndex> {
        /** [Logger] instance used by [VAFIndex]. */
        internal val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)

        /** Key used to store efficiency co-efficient for [VAFIndex]. */
        const val FILTER_EFFICIENCY_CACHE_KEY = "vaf.efficiency"

        /** Default filter efficiency co-efficient for [VAFIndex]. */
        const val DEFAULT_FILTER_EFFICIENCY = 0.9f

        /** False since [VAFIndex] currently doesn't support incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [VAFIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = true

        /** True since [VAFIndex] supports partitioning. */
        override val supportsPartitioning: Boolean = true

        /**
         * Opens a [VAFIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param entity The [Entity] that holds the [VAFIndex].
         * @return The opened [VAFIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): VAFIndex = VAFIndex(name, entity as DefaultEntity)

        /**
         * Generates and returns a [VAFIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [VAFIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<VAFIndex>
            = VAFIndexConfig(parameters[VAFIndexConfig.KEY_MARKS_PER_DIMENSION]?.toIntOrNull() ?: 5)

        /**
         * Returns the [VAFIndexConfig.Binding]
         *
         * @return [VAFIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = VAFIndexConfig.Binding
    }

    /** The [IndexType] of this [VAFIndex]. */
    override val type = IndexType.VAF

    /**
     * Opens and returns a new [VAFIndexRebuilder] object that can be used to rebuild with this [VAFIndex].
     *
     * @param context The [QueryContext] to create this [VAFIndexRebuilder] for.
     * @return [VAFIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext) = VAFIndexRebuilder(this, context)

    /**
     * Opens and returns a new [AsyncVAFIndexRebuilder] object that can be used to rebuild with this [VAFIndex].
     *
     * @param context If the [QueryContext] that requested the [AsyncVAFIndexRebuilder].
     * @return [AsyncVAFIndexRebuilder]
     */
    override fun newAsyncRebuilder(context: QueryContext) = AsyncVAFIndexRebuilder(this, context)

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [VAFIndex].
     *
     * @param parent The [EntityTx] to create this [IndexTx] for.
     */
    override fun newTx(parent: EntityTx): IndexTx {
        require(parent is DefaultEntity.Tx) { "VAFIndex can only be used with DefaultEntity.Tx" }
        return this.Tx(parent)
    }

    /**
     * A [IndexTx] that affects this [VAFIndex].
     */
    inner class Tx(parent: DefaultEntity.Tx) : DefaultIndex.Tx(parent) {

        /** The [EquidistantVAFMarks] object used by this [VAFIndex.Tx]. */
        private var marks: EquidistantVAFMarks

        /** The [Store] associated with this [VAFIndex]. */
        internal var store: Store = this.xodusTx.environment.openStore(this@VAFIndex.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

        init {
            this.marks = this.readMarks()
        }

        /**
         * Internal function used to read [EquidistantVAFMarks] using this [VAFIndex.Tx].
         */
        internal fun readMarks(): EquidistantVAFMarks {
            val structStore = this.xodusTx.environment.openStore(INDEX_STRUCT_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
            val marksRaw = structStore.get(this.xodusTx, NameBinding.Index.toEntry(this@VAFIndex.name))
            return if (marksRaw == null) {
                val config = this.config as VAFIndexConfig
                val statistics = this.transaction.manager.statistics[this.columns[0].name]!!.statistics as RealVectorValueStatistics<*>
                val marks = EquidistantVAFMarks(statistics, config.marksPerDimension)
                structStore.put(this.xodusTx, NameBinding.Index.toEntry(this@VAFIndex.name), EquidistantVAFMarks.Binding.serialize(marks))
                marks
            } else {
                EquidistantVAFMarks.Binding.deserialize(marksRaw)
            }
        }

        /**
         * Internal function used to update [EquidistantVAFMarks] using this [VAFIndex.Tx].
         */
        internal fun writeMarks(marks: EquidistantVAFMarks) {
            val structStore = this.xodusTx.environment.openStore(INDEX_STRUCT_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
            structStore.put(this.xodusTx, NameBinding.Index.toEntry(this@VAFIndex.name), EquidistantVAFMarks.Binding.serialize(marks))
            this.marks = marks
        }

        /**
         * Calculates the cost estimate of this [VAFIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost {
            if (predicate !is ProximityPredicate) return Cost.INVALID
            if (predicate.column != this.columns[0]) return Cost.INVALID
            if (predicate.distance !is MinkowskiDistance<*>) return Cost.INVALID
            val efficiency =  0.9f //TODO Lookup in index statistics
            val signatureRead = this.count()
            val fullRead = (1.0f - efficiency) * signatureRead /* Assumption: Efficiency determines how many entries must be read. */
            return when (predicate) {
                is ProximityPredicate.Scan -> Cost.INVALID
                is ProximityPredicate.ENN -> Cost(
                    Cost.DISK_ACCESS_READ_SEQUENTIAL.io * this.columns[0].type.logicalSize * signatureRead + Cost.DISK_ACCESS_READ_SEQUENTIAL.io * this.columns[0].type.physicalSize * fullRead,
                    (Cost.MEMORY_ACCESS.memory * 2.0f + Cost.FLOP.cpu) * this.columns[0].type.logicalSize * signatureRead + predicate.cost.cpu * fullRead
                )
                is ProximityPredicate.KLimitedSearch -> Cost(
                    Cost.DISK_ACCESS_READ_SEQUENTIAL.io * this.columns[0].type.logicalSize * signatureRead + Cost.DISK_ACCESS_READ_SEQUENTIAL.io * this.columns[0].type.physicalSize * fullRead,
                    (Cost.MEMORY_ACCESS.memory * 2.0f + Cost.FLOP.cpu) * this.columns[0].type.logicalSize * signatureRead + predicate.cost.cpu * fullRead,
                    (Long.SIZE_BYTES + Double.SIZE_BYTES + this.columns[0].type.physicalSize).toFloat() * predicate.k
                )
            }
        }

        /**
         * Returns a [List] of the [ColumnDef] produced by this [VAFIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> {
            require(predicate is ProximityPredicate) { "VAFIndex can only process proximity predicates." }
            return listOf(predicate.distanceColumn, this.columns[0])
        }

        /**
         * Checks if this [VAFIndex] can process the provided [Predicate] and returns true if so and false otherwise.
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean {
            if (predicate !is ProximityPredicate) return false
            if (predicate.column != this.columns[0]) return false
            if (predicate.distance !is MinkowskiDistance) return false
            return (predicate is ProximityPredicate.KLimitedSearch || predicate is ProximityPredicate.ENN)
        }

        /**
         * Returns the map of [Trait]s this [VAFIndex] implements for the given [Predicate]s.
         *
         * @param predicate [Predicate] to check.
         * @return Map of [Trait]s for this [VAFIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = when (predicate) {
            is ProximityPredicate.ENN -> emptyMap()
            is ProximityPredicate.NNS -> mutableMapOf(
                OrderTrait to OrderTrait(listOf(predicate.distanceColumn to SortOrder.ASCENDING)),
                LimitTrait to LimitTrait(predicate.k)
            )
            is ProximityPredicate.FNS -> mutableMapOf(
                OrderTrait to OrderTrait(listOf(predicate.distanceColumn to SortOrder.DESCENDING)),
                LimitTrait to LimitTrait(predicate.k)
            )
            else -> throw IllegalArgumentException("Unsupported predicate for high-dimensional index. This is a programmer's error!")
        }

        /**
         * Truncates this [VAFIndex].
         */
        override fun truncate() = this.txLatch.withLock {
            this.xodusTx.environment.truncateStore(this.store.name, this.xodusTx)
            this.store = this.xodusTx.environment.openStore(this@VAFIndex.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
        }

        /**
         * Drops this [VAFIndex].
         */
        override fun drop() = this.txLatch.withLock {
            /* Drop store. */
            this.xodusTx.environment.removeStore(this.store.name, this.xodusTx)

            /* Delete stored marks. */
            val store = this.xodusTx.environment.openStore(INDEX_STRUCT_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
            store.delete(this.xodusTx, NameBinding.Index.toEntry(this@VAFIndex.name))
            Unit
        }

        /**
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        override fun count(): Long = this.txLatch.withLock {
            this.store.count(this.xodusTx)
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Insert] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method implements the
         * [VAFIndex]'es write model: INSERTS can be applied, if inserted vector lies within the grid obtained upon creation of the index.
         *
         * @param event The [DataEvent.Insert] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean {
            val value = event.data[this.columns[0]] ?: return true

            /* Obtain marks and add them. */
            val sig = this.marks.getSignature(value as RealVectorValue<*>)
            return this.store.add(this.xodusTx, event.tupleId.toKey(), sig.toEntry())
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [VAFIndex] underlying this [VAFIndex.Tx]. This method implements
         * the [VAFIndex]'es [WriteModel]: UPDATES can be applied, if updated vector lies within the grid obtained upon creation of the index.
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val oldValue = event.data[this.columns[0]]?.first
            val newValue = event.data[this.columns[0]]?.second

            /* Obtain marks and update them. */
            return if (newValue is RealVectorValue<*>) { /* Case 1: New value is not null, i.e., update to new value. */
                val newSig = this.marks.getSignature(newValue)
                this.store.put(this.xodusTx, event.tupleId.toKey(), newSig.toEntry())
            } else if (oldValue is RealVectorValue<*>) { /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.store.delete(this.xodusTx, event.tupleId.toKey())
            } else { /* Case 3: There is no value, there was no value, proceed. */
                true
            }
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Delete] to the [VAFIndex] underlying this [VAFIndex.Tx].
         * This method implements the [VAFIndex]'es [WriteModel]: DELETES can always be applied.
         *
         * @param event The [DataEvent.Delete to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Delete): Boolean {
            return event.data[this.columns[0]] == null || this.store.delete(this.xodusTx, event.tupleId.toKey())
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Tuple]s
         * that match the [Predicate]. Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        context(BindingContext)
        override fun filter(predicate: Predicate) = this.txLatch.withLock {
            filter(predicate,this.parent.smallestTupleId() .. this.parent.largestTupleId())
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Tuple]s
         * that match the [Predicate] within the given [LongRange]. Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator].
         */
        context(BindingContext)
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> = this.txLatch.withLock {
            return when(predicate) {
                is ProximityPredicate.ENN -> VAFCursor.ENN(this, this@BindingContext, partition, predicate)
                is ProximityPredicate.FNS -> VAFCursor.FNS(this, this@BindingContext, partition, predicate)
                is ProximityPredicate.NNS -> VAFCursor.NNS(this, this@BindingContext, partition, predicate)
                else -> throw IllegalArgumentException(" VAFIndex can only be used with a NNS, FNS or ENS type proximity predicate. This is a programmer's error!")
            }
        }
    }
}