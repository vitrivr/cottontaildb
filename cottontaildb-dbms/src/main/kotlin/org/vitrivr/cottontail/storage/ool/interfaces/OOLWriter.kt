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
interface OOLWriter<V: Value, D: OutOfLineValue> {
    /** The [OOLFile] this [OOLReader] belongs to. */
    val file: OOLFile<V, D>

    /**
     * Reads a [Value] from the provided [StoredValue].
     *
     * @param value The [Value] [V] to append.
     * @return [StoredValue] for the appended entry.
     */
    fun append(value: V): D

    /**
     * Flushes the data written through this [OOLWriter] to the underlying [OOLFile].
     */
    fun flush()
}