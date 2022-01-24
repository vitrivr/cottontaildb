package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.dbms.general.TxSnapshot

/**
 * This is a [TxSnapshot] implementation for [ColumnTx]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface ColumnTxSnapshot : TxSnapshot {
    /** The delta of entries inserted or deleted. */
    var delta: Long
}