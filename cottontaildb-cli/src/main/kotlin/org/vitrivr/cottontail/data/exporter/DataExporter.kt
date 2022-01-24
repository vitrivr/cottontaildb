package org.vitrivr.cottontail.data.exporter

import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.data.Format
import java.io.Closeable
import java.nio.file.Path

/**
 * A class that can be used for exporting  data out of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface DataExporter : Closeable {
    /** The [Path] to the import file. */
    val path: Path

    /** The [Format] handled by this [DataExporter]. */
    val format: Format

    /** Indicator whether this [DataExporter] has been closed. */
    val closed: Boolean

    /**
     * Offers a single [Tuple] for export by this [DataExporter].
     *
     * @param tuple The [Tuple] to write.
     */
    fun offer(tuple: Tuple)

    /**
     * Offers a [TupleIterator] for export by this [DataExporter].
     *
     * @param iterator The [TupleIterator] to write.
     */
    fun offerAll(iterator: TupleIterator) = iterator.forEach { this.offer(it) }
}