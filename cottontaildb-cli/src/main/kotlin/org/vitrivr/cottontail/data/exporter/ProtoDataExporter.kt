package org.vitrivr.cottontail.data.exporter

import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.importer.ProtoDataImporter
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataExporter] implementation that can be used to write a [Format.PROTO] files containing a list of entries.
 *
 * Data produced by the [ProtoDataExporter] can be used as input for the [ProtoDataImporter].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ProtoDataExporter(override val path: Path) : DataExporter {
    /** The [Format] handled by this [DataExporter]. */
    override val format: Format = Format.PROTO

    /** Indicator whether this [DataExporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** Internal [OutputStream] */
    private val output =
        Files.newOutputStream(this.path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

    /**
     * Offers a new [Tuple] to this [ProtoDataExporter], which will be appended to the output stream.
     *
     * @param tuple The [Tuple] to append.
     */
    override fun offer(tuple: Tuple) {
        val insert = CottontailGrpc.InsertMessage.newBuilder()
        for (i in (0 until tuple.size())) {
            val name = tuple.nameForIndex(i)
            val value = tuple[i]?.toGrpc()
            val builder = insert.addElementsBuilder()
            if (name.contains('.')) {
                builder.setColumn(CottontailGrpc.ColumnName.newBuilder().setName(name.split('.').last()))
            } else {
                builder.setColumn(CottontailGrpc.ColumnName.newBuilder().setName(name))
            }
            if (value != null) {
                builder.value = value
            } else {
                builder.value = CottontailGrpc.Literal.getDefaultInstance()
            }
        }
        insert.build().writeDelimitedTo(this.output)
    }

    /**
     * Closes this [ProtoDataExporter].
     */
    override fun close() {
        if (!this.closed) {
            this.output.close()
            this.closed = true
        }
    }
}