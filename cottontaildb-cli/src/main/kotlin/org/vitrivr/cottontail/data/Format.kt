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
 * @version 1.0.1
 */
enum class Format(val suffix: String) {
    /** ProtocolBuffer format. */
    PROTO("proto"),

    /** JSON format. */
    JSON("json"),

    /** CSV format. */
    CSV("csv");

    /**
     * Creates and returns a new [DataImporter] for this [Format].
     *
     * @param file [Path] to input file.
     * @param schema Optional list of [ColumnDef]s that should be imported (not required).
     * @param fuzzy Optional flag to use fuzzy import (e.g. try best effort and skip corrupt entries)
     * @return [DataImporter]
     */
    fun newImporter(file: Path, schema: List<ColumnDef<*>>, fuzzy:Boolean=false): DataImporter = when (this) {
        PROTO -> ProtoDataImporter(file, schema)
        JSON -> JsonDataImporter(file, schema)
        CSV -> CSVDataImporter(file, schema, fuzzy)
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
