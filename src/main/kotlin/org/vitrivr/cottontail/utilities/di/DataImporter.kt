package org.vitrivr.cottontail.utilities.di

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import java.nio.file.Path

/**
 * A class that can be used for parsing and importing external data into Cottontail DB. Basically
 * and extended [Iterator] for [CottontailGrpc.InsertMessage]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DataImporter : Iterator<CottontailGrpc.InsertMessage.Builder>, AutoCloseable {
    /** The [Path] to the import file. */
    val path: Path

    /** The [ColumnDef]s imported by this [DataImporter]. */
    val schema: Array<ColumnDef<*>>

    /** The [ImportFormat] handled by this [DataImporter]. */
    val format: ImportFormat

    /** Indicator whether this [DataImporter] has been closed. */
    val closed: Boolean
}