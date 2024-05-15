package org.vitrivr.cottontail.dbms.entity.values

import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.RowId

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface StoredValueRef {
    /** A reference to a null value. */
    data object Null: StoredValueRef

    @JvmInline
    value class Inline<T: Value>(val value: T): StoredValueRef

    /**
     *
     */
    sealed interface OutOfLine: StoredValueRef {
        /** A reference to a fixed value. */
        @JvmInline
        value class Fixed(val rowId: RowId): OutOfLine

        /** A reference to a variable value. */
        class Variable(val position: Long, val size: Int): OutOfLine
    }
}