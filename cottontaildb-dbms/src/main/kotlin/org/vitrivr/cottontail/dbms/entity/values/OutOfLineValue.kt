package org.vitrivr.cottontail.dbms.entity.values

import org.vitrivr.cottontail.dbms.entity.RowId

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface OutOfLineValue {
    /** A reference to a fixed value. */
    @JvmInline
    value class Fixed(val rowId: RowId): OutOfLineValue

    /** A reference to a variable value. */
    class Variable(val position: Long, val size: Int): OutOfLineValue
}