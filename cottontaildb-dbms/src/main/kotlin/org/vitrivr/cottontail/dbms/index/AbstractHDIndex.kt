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
    protected abstract inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [AuxiliaryValueCollection] used by this [AbstractHDIndex.Tx]. */
        protected abstract val auxiliary: AuxiliaryValueCollection

        /**
         * Tries to process an incoming [Operation.DataManagementOperation]. Such [Operation.DataManagementOperation] can
         * either be appended to the underlying index (if the write-model allows for it) or appended to the auxiliary value list.
         *
         * @param event [Operation.DataManagementOperation]
         */
        final override fun update(event: Operation.DataManagementOperation) = this.txLatch.withLock {
            /* If index does not support incremental updating at all. */
            if (!this@AbstractHDIndex.supportsIncrementalUpdate) {
                this.updateState(IndexState.STALE)
                return@withLock
            }

            /* If write-model does not allow propagation, apply change to auxiliary value collection. */
            if (!this.tryApplyToIndex(event)) {
                when (event) {
                    is Operation.DataManagementOperation.DeleteOperation -> this.auxiliary.applyDelete(event.tupleId)
                    is Operation.DataManagementOperation.InsertOperation -> {
                        val value = event.inserts[this@AbstractHDIndex.column]
                        if (value is VectorValue<*>) this.auxiliary.applyInsert(event.tupleId)
                    }
                    is Operation.DataManagementOperation.UpdateOperation -> {
                        val value = event.updates[this@AbstractHDIndex.column]?.second
                        if (value is VectorValue<*>) {
                            this.auxiliary.applyUpdate(event.tupleId)
                        } else if (value == null) {
                            this.auxiliary.applyDelete(event.tupleId)
                        }
                    }
                }
                this.updateState(IndexState.DIRTY)
                return@withLock
            }
        }

        /**
         * Tries to apply the change applied by this [Operation.DataManagementOperation] to this [AbstractHDIndex]. This is the
         * implementation of this [AbstractHDIndex]'s write model.
         *
         * This method returns true if, and only if, a change can be applied to an index without impairing its ability to produce correct results.
         * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
         *
         * @param event The [Operation.DataManagementOperation] to apply.
         * @return True if change could be applied, false otherwise.
         */
        protected abstract fun tryApplyToIndex(event: Operation.DataManagementOperation): Boolean
    }
}