package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.*
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.data.Dumper
import org.vitrivr.cottontail.data.Format
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to dump the content of an entity into a file.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class DumpEntityCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "dump", help = "Dumps the content of an entity to disk.") {
    /** Path to the output folder or file. */
    private val out: Path by option(
        "-o",
        "--out",
        help = "Path to the output folder or file."
    ).convert { Paths.get(it) }.required()

    /** Export format. Defaults to CBOR. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Export format. Defaults to CBOR."
    ).convert { Format.valueOf(it) }.default(Format.CBOR)

    /** The default batch size for dumping data. */
    private val batchSize: Int by option(
        "-b",
        "--batch",
        help = "Export format. Defaults to CBOR."
    ).convert { it.toInt() }.default(100000)

    /** Flag indicating whether output should be compressed. */
    private val compress: Boolean by option(
        "-c",
        "--compress",
        help = "Whether export should be compressed)"
    ).flag(default = true)

    override fun exec() {
        val dumper = if (this.compress) {
            Dumper.Zip(this.client, this.out, this.format, this.batchSize)
        } else {
            Dumper.Folder(this.client, this.out, this.format, this.batchSize)
        }
        dumper.use {
            val (duration, count) = measureTimedValue {
                it.dump(this.entityName)
            }
            println("Completed! Dumping ${this.entityName} ($count entries) took $duration.")
        }
    }
}