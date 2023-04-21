package org.vitrivr.cottontail.data

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.data.exporter.CSVDataExporter
import org.vitrivr.cottontail.data.exporter.DataExporter
import org.vitrivr.cottontail.data.exporter.JsonDataExporter
import org.vitrivr.cottontail.data.exporter.ProtoDataExporter
import org.vitrivr.cottontail.data.importer.CSVDataImporter
import org.vitrivr.cottontail.data.importer.DataImporter
import org.vitrivr.cottontail.data.importer.JsonDataImporter
import org.vitrivr.cottontail.data.importer.ProtoDataImporter
import java.nio.file.Path

/**
 * Data import and export format used and supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class Format(val suffix: String) {
    /** ProtocolBuffer format. */
    PROTO("proto"),

    /** JSON format. */
    JSON("json"),

    /** CSV format. */
    CSV("csv");

    companion object {
        /**
         * Tries to detect the [Format] for the file under the given [Path] by comparing its suffix.
         *
         * @param path The [Path] to check.
         * @return The detected [Format].
         */
        fun detectFormatForPath(path: Path) = when {
            path.toString().endsWith(PROTO.suffix) -> PROTO
            path.toString().endsWith(JSON.suffix) -> JSON
            path.toString().endsWith(CSV.suffix) -> CSV
            else -> throw IllegalArgumentException("Input format of file $path could not be detected (supported: ${Format.values().joinToString(", ")}).")
        }
    }

    /**
     * Creates and returns a new [DataImporter] for this [Format].
     *
     * @param file [Path] to input file.
     * @param schema Optional list of [ColumnDef]s that should be imported (not required).
     * @return [DataImporter]
     */
    fun newImporter(file: Path, schema: List<ColumnDef<*>>): DataImporter = when (this) {
        PROTO -> ProtoDataImporter(file, schema)
        JSON -> JsonDataImporter(file, schema)
        CSV -> CSVDataImporter(file, schema)
    }

    /**
     * Creates and returns a new [DataExporter] for this [Format].
     *
     * @param file [Path] to input file.
     * @return [DataExporter]
     */
    fun newExporter(file: Path): DataExporter = when (this) {
        PROTO -> ProtoDataExporter(file)
        JSON -> JsonDataExporter(file)
        CSV -> CSVDataExporter(file)
    }
}