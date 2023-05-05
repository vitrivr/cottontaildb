package org.vitrivr.cottontail.data.exporter

import com.google.gson.stream.JsonWriter
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.importer.JsonDataImporter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataExporter] implementation that can be used to write a [Format.JSON] files containing an array of entries.
 *
 * Data produced by the [JsonDataExporter] can be used as input for the [JsonDataImporter].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class JsonDataExporter(override val path: Path, val indent: String = "") : DataExporter {
    /** The [Format] handled by this [DataExporter]. */
    override val format: Format = Format.JSON

    /** Indicator whether this [DataExporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** The [JsonWriter] instance used to read the JSON file. */
    private val writer = Files.newBufferedWriter(this.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

    /** Flag indicating, if this is the first value being written. */
    private var first: Boolean = true

    init {
      this.writer.append("[\n")
    }

    /**
     * Offers a new [Tuple] to this [ProtoDataExporter], which will
     * be appended to the output stream.
     *
     * @param tuple The [Tuple] to append.
     */
    @Suppress("UNCHECKED_CAST")
    override fun offer(tuple: Tuple) {
        if (this.first) {
            this.writer.append("\n${this.indent}" + Json.encodeToString(tuple))
            this.first = false
        } else {
            this.writer.append(",\n${this.indent}" +  Json.encodeToString(tuple))
        }
    }
    /**
     * Closes this [ProtoDataExporter].
     */
    override fun close() {
        if (!this.closed) {
            this.writer.append("]")
            this.writer.close()
            this.closed = true
        }
    }
}