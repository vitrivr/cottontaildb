package org.vitrivr.cottontail.data.importer

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.io.Closeable
import java.nio.file.Path

/**
 * A class that can be used for parsing and importing external data into Cottontail DB. Basically
 * and extended [Iterator] for [CottontailGrpc.InsertMessage]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface DataImporter : Iterator<Map<ColumnDef<*>, Value?>>, Closeable {
    /** The [Path] to the import file. */
    val path: Path

    /** The [Format] handled by this [DataImporter]. */
    val format: Format

    /** Indicator whether this [DataImporter] has been closed. */
    val closed: Boolean

    /** The [ColumnDef] that are populated by this [DataImporter]  */
    val schema: List<ColumnDef<*>>
}