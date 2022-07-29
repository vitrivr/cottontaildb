package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.LimitTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.va.rebuilder.AsyncVAFIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.rebuilder.VAFIndexRebuilder
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.concurrent.withLock

/**
 * An [AbstractIndex] structure for proximity based search (NNS / FNS) that uses a vector approximation (VA) file ([1]). Can be used for all types of [RealVectorValue]s
 * and all Minkowski metrics (L1, L2, Lp etc.).
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.3.0
 */
class VAFIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [VAFIndex].
     */
    companion object: IndexDescriptor<VAFIndex> {
        /** [Logger] instance used by [VAFIndex]. */
        internal val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)

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
         * Initializes the [Store] for a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val store = (catalogue as DefaultCatalogue).environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize VAF index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            (catalogue as DefaultCatalogue).environment.removeStore(name.storeName(), context.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize VAF index $name due to an exception: ${e.message}.")
            false
        }

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
     * @return [AsyncVAFIndexRebuilder]
     */
    override fun newAsyncRebuilder() = AsyncVAFIndexRebuilder(this)

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [VAFIndex].
     *
     * @param context The [QueryContext] to create this [IndexTx] for.
     */
    override fun newTx(context: QueryContext): IndexTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * A [IndexTx] that affects this [VAFIndex].
     */
    inner class Tx(context: QueryContext) : AbstractIndex.Tx(context) {

        /** The [EquidistantVAFMarks] object used by this [VAFIndex.Tx]. */
        internal val marks: EquidistantVAFMarks by lazy {
            IndexStructCatalogueEntry.read(this@VAFIndex.name, this@VAFIndex.catalogue, this.context.txn.xodusTx, EquidistantVAFMarks.Binding)?:
                throw DatabaseException.DataCorruptionException("Marks for VAF index ${this@VAFIndex.name} are missing.")
        }

        /** The Xodus [Store] used to store [VAFSignature]s. */
        internal val dataStore: Store by lazy {
            this@VAFIndex.catalogue.environment.openStore(this@VAFIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.txn.xodusTx, false) ?:
                throw DatabaseException.DataCorruptionException("Store for VAF index ${this@VAFIndex.name} is missing.")
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
            val signatureRead = this.count()
            val fullRead = 0.1f * signatureRead /* Assumption: Only 10% of values are read --> see paper. */
            return when (predicate) {
                is ProximityPredicate.Scan -> Cost.INVALID
                is ProximityPredicate.ENN -> Cost(
                    Cost.DISK_ACCESS_READ.io * this.columns[0].type.logicalSize * signatureRead + Cost.DISK_ACCESS_READ.io * this.columns[0].type.physicalSize * fullRead,
                    (Cost.MEMORY_ACCESS.memory * 2.0f + Cost.FLOP.cpu) * this.columns[0].type.logicalSize * signatureRead + predicate.cost.cpu * fullRead,
                    (Long.SIZE_BYTES + Double.SIZE_BYTES + this.columns[0].type.physicalSize).toFloat() * 10_000
                )
                is ProximityPredicate.KLimitedSearch -> Cost(
                    Cost.DISK_ACCESS_READ.io * this.columns[0].type.logicalSize * signatureRead + Cost.DISK_ACCESS_READ.io * this.columns[0].type.physicalSize * fullRead,
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
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.txn.xodusTx)
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
            return this.dataStore.add(this.context.txn.xodusTx, event.tupleId.toKey(), this.marks.getSignature(value as RealVectorValue<*>).toEntry())
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
                this.dataStore.put(this.context.txn.xodusTx, event.tupleId.toKey(), this.marks.getSignature(newValue).toEntry())
            } else if (oldValue is RealVectorValue<*>) { /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.dataStore.delete(this.context.txn.xodusTx, event.tupleId.toKey())
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
            return event.data[this.columns[0]] == null || this.dataStore.delete(this.context.txn.xodusTx, event.tupleId.toKey())
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
        override fun filter(predicate: Predicate) = this.txLatch.withLock {
            val entityTx = this@VAFIndex.parent.newTx(this.context)
            filter(predicate,entityTx.smallestTupleId() .. entityTx.largestTupleId())
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] within the given [LongRange]. Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator].
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> = this.txLatch.withLock {
            return when(predicate) {
                is ProximityPredicate.ENN -> VAFCursor.ENN(partition, predicate, this)
                is ProximityPredicate.FNS -> VAFCursor.FNS(partition, predicate, this)
                is ProximityPredicate.NNS -> VAFCursor.NNS(partition, predicate, this)
                else -> throw IllegalArgumentException(" VAFIndex can only be used with a NNS, FNS or ENS type proximity predicate. This is a programmer's error!")
            }
        }
    }
}