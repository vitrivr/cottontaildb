package org.vitrivr.cottontail.dbms.entity

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.general.TxSnapshot
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.statistics.entity.EntityStatistics

/**
 * This is a [TxSnapshot] implementation for [EntityTx]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface EntityTxSnapshot : TxSnapshot {
    /** The state of the enclosing [Entity] as of the start of the [EntityTx]. */
    val statistics: EntityStatistics

    /** A map of all [Index] structures available to the enclosing [EntityTx]. */
    val indexes: MutableMap<Name.IndexName, Index>
}