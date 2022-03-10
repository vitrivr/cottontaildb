package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.index.basics.avc.AuxiliaryValueCollection
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

    init {
        require(this.columns.size == 1) { "High-dimensional indexes do not support indexing of more than a single column." }
        require(this.columns[0].type is Types.Vector<*,*>) { "High-dimensional indexes only support indexing of vector columns."}
    }

    /** The [ColumnDef] that is being indexed by this [AbstractHDIndex]. */
    val column: ColumnDef<*>
        get() = this.columns[0]

    /** The dimensionality of this [AbstractHDIndex]. */
    val dimension: Int
        get() = this.column.type.logicalSize

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context), WriteModel {

        /** The [AuxiliaryValueCollection] used by this [AbstractHDIndex.Tx]. */
        protected abstract val auxiliary: AuxiliaryValueCollection

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
                val value = operation.inserts[this@AbstractHDIndex.column]
                if (value is VectorValue<*>) this.auxiliary.applyInsert(operation.tupleId)

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
                val value = operation.updates[this@AbstractHDIndex.column]?.second
                if (value is VectorValue<*>) {
                    this.auxiliary.applyUpdate(operation.tupleId)
                } else if (value == null) {
                    this.auxiliary.applyDelete(operation.tupleId)
                }

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
                this.auxiliary.applyDelete(operation.tupleId)
                this.updateState(IndexState.DIRTY)
            }
        }
    }
}