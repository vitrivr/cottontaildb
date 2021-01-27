package org.vitrivr.cottontail.utilities.data.exporter

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.nio.file.Path

/**
 * A class that can be used for exporting  data out of Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DataExporter : AutoCloseable {
    /** The [Path] to the import file. */
    val path: Path

    /** The [Format] handled by this [DataImporter]. */
    val format: Format

    /** Indicator whether this [DataImporter] has been closed. */
    val closed: Boolean

    /**
     *
     */
    fun offer(message: CottontailGrpc.QueryResponseMessage)
}