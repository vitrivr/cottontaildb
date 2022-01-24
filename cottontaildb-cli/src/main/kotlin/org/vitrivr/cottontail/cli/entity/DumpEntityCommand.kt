package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.data.Format
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to dump the content of an entity into a file.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class DumpEntityCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "dump", help = "Dumps the content of an entire entity to disk.") {

    /** Flag indicating, whether query should be executed or explained. */
    private val out: Path by option(
        "-o",
        "--out",
        help = "Path to the output folder (file name will be determined by entity and format)."
    ).convert { Paths.get(it) }.required()

    /** Flag indicating, whether query should be executed or explained. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Export format. Defaults to PROTO."
    ).convert { Format.valueOf(it) }.default(Format.PROTO)

    override fun exec() {
        val qm = Query(this.entityName.toString())
        val path = this.out.resolve("${this.entityName}.${this.format.suffix}")
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
            println("Dumping ${this.entityName} took $duration.")
        } catch (e: Throwable) {
            print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
        }
    }
}