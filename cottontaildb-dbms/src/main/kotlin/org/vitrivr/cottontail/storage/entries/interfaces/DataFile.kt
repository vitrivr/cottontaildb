package org.vitrivr.cottontail.storage.entries.interfaces

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import java.io.Closeable
import java.nio.file.Path

/**
 * A Cottontail DB HARE data file. A [DataFile] is used to store [Value]s in an append-only file.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DataFile<V: Value, D: StoredValueRef>: Closeable {
    companion object {
        const val SEGMENT_SIZE = 16_000_000 /* A segment size of 16 MB for off-line storage. */
    }

    /** The [Path] to the Cottontail DB HARE data file. */
    val path: Path

    /** The [Types] to the Cottontail DB HARE data file. */
    val type: Types<V>

    /**
     * Provides a [Reader] for this [DataFile].
     *
     * @param pattern [ReaderPattern] to use for reading.
     * @return [Reader]
     */
    fun reader(pattern: ReaderPattern): Reader<V,D>

    /**
     * Provides a [Writer] for this [DataFile].
     *
     * @return [Writer]
     */
    fun writer(): Writer<V,D>
}