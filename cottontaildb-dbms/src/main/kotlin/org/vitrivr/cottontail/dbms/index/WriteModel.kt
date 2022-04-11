package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.dbms.operations.Operation

/**
 * A [WriteModel], usually used for an [IndexTx]. A [WriteModel] offers facilities to process [Operation.DataManagementOperation] and
 * determine, whether those [Operation.DataManagementOperation] can be applied.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface WriteModel {
    /**
     * Tries to apply the change applied by this [Operation.DataManagementOperation.InsertOperation] to this [WriteModel].
     *
     * This method returns true if, and only if, the INSERT can be applied to an index without impairing its ability to produce correct results.
     * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
     *
     * @param operation The [Operation.DataManagementOperation.InsertOperation] to apply.
     * @return True if change could be applied, false otherwise.
     */
    fun tryApply(operation: Operation.DataManagementOperation.InsertOperation): Boolean

    /**
     * Tries to apply the change applied by this [Operation.DataManagementOperation.UpdateOperation] to this [WriteModel].
     *
     * This method returns true if, and only if, a change can be applied to an index without impairing its ability to produce correct results.
     * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
     *
     * @param operation The [Operation.DataManagementOperation.UpdateOperation] to apply.
     * @return True if change could be applied, false otherwise.
     */
    fun tryApply(operation: Operation.DataManagementOperation.UpdateOperation): Boolean

    /**
     * Tries to apply the change applied by this [Operation.DataManagementOperation.DeleteOperation] to this [WriteModel].
     *
     * This method returns true if, and only if, a change can be applied to an index without impairing its ability to produce correct results.
     * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
     *
     * @param operation The [Operation.DataManagementOperation.DeleteOperation] to apply.
     * @return True if change could be applied, false otherwise.
     */
    fun tryApply(operation: Operation.DataManagementOperation.DeleteOperation): Boolean
}