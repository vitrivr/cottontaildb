package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.index.Index

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface EntityTxSnapshot : TxSnapshot {
    /**
     * The sum of the number of entries added or deleted by the [EntityTx].
     *
     * For performance reasons, this is only persisted upon commit.
     */
    var delta: Long

    /**
     * List of [Index]es created during this [EntityTxState].
     *
     * Such [Index]es are only available to the [EntityTx] that created them.
     */
    val createdIndexes: MutableList<Index>

    /**
     * List of [Index]es dropped during this [EntityTxState]. Tracked for
     */
    val droppedIndexes: MutableList<Index>
}