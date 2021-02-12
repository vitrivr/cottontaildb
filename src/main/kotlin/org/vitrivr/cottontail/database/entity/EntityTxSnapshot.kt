package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.model.basics.Name

/**
 * This is a [TxSnapshot] implementation for [EntityTx]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface EntityTxSnapshot : TxSnapshot {
    /**
     * The sum of the number of entries added or deleted by the [EntityTx].
     *
     * For performance reasons, this is only persisted upon commit.
     */
    var delta: Long

    /** A map of all [Index] structures available to the enclosing [EntityTx]. */
    val indexes: MutableMap<Name.IndexName, Index>
}