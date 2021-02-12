package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.database.general.TxSnapshot

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ColumnTxSnapshot : TxSnapshot {
    /** */
    var delta: Long
}