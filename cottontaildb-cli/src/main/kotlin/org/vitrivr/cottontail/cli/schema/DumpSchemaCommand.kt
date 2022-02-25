package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.extensions.proto
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to dump the content of all entities within a schema into individual files.
 *
 * @author Florian Spiess
 * @version 2.0.0
 */
@ExperimentalTime
class DumpSchemaCommand(client: SimpleClient) : AbstractCottontailCommand.Schema(
    client,
    name = "dump",
    help = "Dumps the content of all entities within a schema into individual files on disk."
) {

    /** Path to the output folder (file name will be determined by entity and format). */
    private val out: Path by option(
        "-o",
        "--out",
        help = "Path to the output folder (file name will be determined by entity and format)."
    ).convert { Paths.get(it) }.required()

    /** Export format. Defaults to PROTO. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Export format. Defaults to PROTO."
    ).convert { Format.valueOf(it) }.default(Format.PROTO)

    override fun exec() {
        val entities = client.list(CottontailGrpc.ListEntityMessage.newBuilder().setSchema(schemaName.proto()).build())
            .asSequence().map { x -> x[0].toString() }.toList()

        for (entity in entities) {
            val qm = Query(entity)
            val path = this.out.resolve("${entity}.${this.format.suffix}")
            val dataExporter = this.format.newExporter(path)

            try {
                val duration = measureTime {
                    /* Execute query and export data. */
                    val results = this.client.query(qm)
                    for (r in results) {
                        dataExporter.offer(r)
                    }
                    dataExporter.close()
                }
                println("Dumping $entity took $duration.")
            } catch (e: Throwable) {
                print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
            }
        }
    }
}