package org.vitrivr.cottontail.storage.ool.interfaces

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import java.nio.file.Path

/**
 * A Cottontail DB HARE out-of-line (OOL) data file.
 *
 * [OOLFile]s are used to store [Value]s out-of-line (i.e., not as part of the record) in an append-only file.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface OOLFile<V: Value, D: StoredValueRef> {
    companion object {
        const val SEGMENT_SIZE = 16_000_000 /* A segment size of 16 MB for off-line storage. */
    }

    /** The [Path] to the Cottontail DB HARE data file. */
    val path: Path

    /** The [Types] to the Cottontail DB HARE data file. */
    val type: Types<V>

    /**
     * Provides a [OOLReader] for this [OOLFile].
     *
     * @param pattern [AccessPattern] to use for reading.
     * @return [OOLReader]
     */
    fun reader(pattern: AccessPattern): OOLReader<V,D>

    /**
     * Provides a [OOLWriter] for this [OOLFile].
     *
     * @return [OOLWriter]
     */
    fun writer(): OOLWriter<V,D>
}