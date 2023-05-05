package org.vitrivr.cottontail.data.exporter

import com.github.doyaaaaaken.kotlincsv.client.KotlinCsvExperimental
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.toCsv
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.importer.DataImporter
import org.vitrivr.cottontail.data.importer.JsonDataImporter
import java.nio.file.Path

/**
 * A [DataExporter] implementation that can be used to write a [Format.CSV] files containing an array of entries.
 *
 * Data produced by the [JsonDataExporter] can be used as input for the [JsonDataImporter].
 *
 * @author Luca Rossetto
 * @version 1.0.1
 */
class CSVDataExporter(override val path: Path) : DataExporter {

    @Suppress("EXPERIMENTAL_IS_NOT_ENABLED")
    @OptIn(KotlinCsvExperimental::class)
    private val writer = csvWriter().openAndGetRawWriter(path.toFile())

    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.CSV

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    private var header: List<String>? = null

    override fun offer(tuple: Tuple) {
        if (header == null) {
            header = (0..tuple.size()).map { tuple.nameForIndex(it) }
            this.writer.writeRow(header)
        }
        this.writer.writeRow(tuple.toCsv())
    }

    override fun close() {
        writer.close()
        this.closed = true
    }
}