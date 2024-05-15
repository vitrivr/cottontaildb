package org.vitrivr.cottontail.storage.entries.interfaces

import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef

/**
 * A [Reader] is used to read [Value]s from a [DataFile].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Writer<V: Value, D: StoredValueRef> {
    /** The [DataFile] this [Reader] belongs to. */
    val file: DataFile<V, D>

    /**
     * Reads a [Value] from the provided [StoredValue].
     *
     * @param value The [Value] [V] to append.
     * @return [StoredValue] for the appended entry.
     */
    fun append(value: V): D

    /**
     * Flushes the data written through this [Writer] to the underlying [DataFile].
     */
    fun flush()
}