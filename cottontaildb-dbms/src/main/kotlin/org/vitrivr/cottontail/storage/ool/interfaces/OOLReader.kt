package org.vitrivr.cottontail.storage.ool.interfaces

import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.OutOfLineValue
import org.vitrivr.cottontail.dbms.entity.values.StoredValue

/**
 * A [OOLReader] is used to read [Value]s from a [OOLFile].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface OOLReader<V: Value, D: OutOfLineValue> {
    /** The [OOLFile] this [OOLReader] belongs to. */
    val file: OOLFile<V, D>

    /**
     * Reads a [Value] from the provided [StoredValue].
     *
     * @param row [StoredValue] that specifies the row to read from.
     * @return [Value]
     */
    fun read(row: D): V
}