package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.dbms.events.DataEvent

/**
 * A [WriteModel], usually used for an [IndexTx].
 *
 * A [WriteModel] offers facilities to process [DataEvent] and determine, whether those [DataEvent] can be applied.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface WriteModel {
    /**
     * Tries to apply the change applied by this [DataEvent.Insert] to this [WriteModel].
     *
     * This method returns true if, and only if, the INSERT can be applied to an index without impairing its ability to produce correct results.
     * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
     *
     * @param event The [DataEvent.Insert] to apply.
     * @return True if change could be applied, false otherwise.
     */
    fun tryApply(event: DataEvent.Insert): Boolean

    /**
     * Tries to apply the change applied by this [DataEvent.Update] to this [WriteModel].
     *
     * This method returns true if, and only if, a change can be applied to an index without impairing its ability to produce correct results.
     * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
     *
     * @param event The [DataEvent.Update] to apply.
     * @return True if change could be applied, false otherwise.
     */
    fun tryApply(event: DataEvent.Update): Boolean

    /**
     * Tries to apply the change applied by this [DataEvent.Delete] to this [WriteModel].
     *
     * This method returns true if, and only if, a change can be applied to an index without impairing its ability to produce correct results.
     * In such a case the change is applied to the underlying data structure. In all other cases, no changes are applied and the method returns false.
     *
     * @param event The [DataEvent.Delete] to apply.
     * @return True if change could be applied, false otherwise.
     */
    fun tryApply(event: DataEvent.Delete): Boolean
}