package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.nodes.traits.LimitTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basics.avc.AuxiliaryValueCollection
import org.vitrivr.cottontail.dbms.index.gg.GGIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import kotlin.concurrent.withLock

/**
 * An [Index] implementation that outlines the fundamental structure of a high-dimensional (HD) index.
 *
 * Implementations of HD [Index] in Cottontail DB should inherit from this class.
 *
 * @see AbstractIndex
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractHDIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context), WriteModel {

        /** The [ColumnDef] that is being indexed by this [AbstractHDIndex]. */
        val column: ColumnDef<*>
            get() = this.columns[0]

        /** Set of supported [Name.FunctionName] that can act as distance functions. */
        protected abstract val supportedDistances: Set<Signature.Closed<*>>

        /**
         * Checks if this [GGIndex] can process the provided [Predicate] and returns true if so and false otherwise.
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean {
            return predicate is ProximityPredicate
                    && predicate.column == this.column && predicate.distance.signature in this.supportedDistances
        }

        /**
         * Calculates the cost estimate if this [GGIndex] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = when (predicate) {
            is ProximityPredicate.NNS -> mapOf(
                OrderTrait to OrderTrait(listOf(predicate.distanceColumn to SortOrder.ASCENDING)),
                LimitTrait to LimitTrait(predicate.k.toLong())
            )
            is ProximityPredicate.FNS -> mapOf(
                OrderTrait to OrderTrait(listOf(predicate.distanceColumn to SortOrder.DESCENDING)),
                LimitTrait to LimitTrait(predicate.k.toLong())
            )
            else -> throw IllegalArgumentException("Unsupported predicate for HD-index. This is a programmer's error!")
        }

        /**
         * Tries to process an incoming [Operation.DataManagementOperation.InsertOperation].
         *
         * If the [AbstractHDIndex] does not support incremental updates, the [AbstractHDIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is either propagated to the [AbstractHDIndex] or to the [AuxiliaryValueCollection] this marking the
         * [AbstractIndex] as [IndexState.DIRTY].
         *
         * @param operation [Operation.DataManagementOperation.UpdateOperation] that should be processed,
         */
        final override fun insert(operation: Operation.DataManagementOperation.InsertOperation) = this.txLatch.withLock {
            /* If index does not support incremental updating at all. */
            if (!this@AbstractHDIndex.supportsIncrementalUpdate) {
                this.updateState(IndexState.STALE)
                return@withLock
            }

            /* If write-model does not allow propagation, apply change to auxiliary value collection. */
            if (!this.tryApply(operation)) {
                val value = operation.inserts[this.column]

                /* TODO: Process. */

                /* Update index state. */
                this.updateState(IndexState.DIRTY)
            }
        }

        /**
         * Tries to process an incoming [Operation.DataManagementOperation.UpdateOperation].
         *
         * If the [AbstractHDIndex] does not support incremental updates, the [AbstractHDIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is either propagated to the [AbstractHDIndex] or to the [AuxiliaryValueCollection] this marking the
         * [AbstractIndex] as [IndexState.DIRTY].
         *
         * @param operation [Operation.DataManagementOperation.UpdateOperation]
         */
        final override fun update(operation: Operation.DataManagementOperation.UpdateOperation) = this.txLatch.withLock {
            /* If index does not support incremental updating at all. */
            if (!this@AbstractHDIndex.supportsIncrementalUpdate) {
                this.updateState(IndexState.STALE)
                return@withLock
            }

            /* If write-model does not allow propagation, apply change to auxiliary value collection. */
            if (!this.tryApply(operation)) {
                val value = operation.updates[this.column]?.second

                /* TODO: Process. */

                /* Update index state. */
                this.updateState(IndexState.DIRTY)
            }
        }

        /**
         * Tries to process an incoming [Operation.DataManagementOperation.DeleteOperation].
         *
         * If the [AbstractHDIndex] does not support incremental updates, the [AbstractHDIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is either propagated to the [AbstractHDIndex] or to the [AuxiliaryValueCollection] this marking the
         * [AbstractIndex] as [IndexState.DIRTY].
         *
         * @param operation [Operation.DataManagementOperation.DeleteOperation] that should be processed.
         */
        final override fun delete(operation: Operation.DataManagementOperation.DeleteOperation) = this.txLatch.withLock {
            /* If index does not support incremental updating at all. */
            if (!this@AbstractHDIndex.supportsIncrementalUpdate) {
                this.updateState(IndexState.STALE)
                return@withLock
            }

            /* If write-model does not allow propagation, apply change to auxiliary value collection. */
            if (!this.tryApply(operation)) {

                /* TODO: Process. */

                this.updateState(IndexState.DIRTY)
            }
        }
    }
}