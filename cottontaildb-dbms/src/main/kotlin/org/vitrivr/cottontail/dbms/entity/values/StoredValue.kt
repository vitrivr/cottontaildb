package org.vitrivr.cottontail.dbms.entity.values

import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.storage.entries.interfaces.Reader

/**
 * A reference to a [StoredValue.OutOfLine].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface StoredValue<T: Value> {

    /** The [Value]*/
    val value: T?

    @JvmInline
    value class Inline<T: Value>(override val value: T): StoredValue<T> {
        override fun toString(): String = value.toString()
    }

    /**
     *
     */
    sealed interface OutOfLine<T: Value>: StoredValue<T> {
        /** A reference to a fixed value. */
        class Fixed<V: Value>(private val ref: StoredValueRef.OutOfLine.Fixed, val reader: Reader<V, StoredValueRef.OutOfLine.Fixed>): OutOfLine<V> {
            override val value: V by lazy { this.reader.read(this.ref) }
        }

        /** A reference to a variable value. */
        class Variable<V: Value>(private val ref: StoredValueRef.OutOfLine.Variable, val reader: Reader<V, StoredValueRef.OutOfLine.Variable>): OutOfLine<V> {
            override val value: V by lazy { this.reader.read(this.ref) }
        }
    }
}