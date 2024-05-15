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
interface Reader<V: Value, D: StoredValueRef> {
    /** The [DataFile] this [Reader] belongs to. */
    val file: DataFile<V, D>

    /**
     * Reads a [Value] from the provided [StoredValue].
     *
     * @param row [StoredValue] that specifies the row to read from.
     * @return [Value]
     */
    fun read(row: D): V
}