package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
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
    companion object {
        /**
         * Dumps the content entity into a file. This method has been separated from the [AboutEntityCommand] instance for testability.
         *
         * @param entityName The [Name.EntityName] of the entity to dump.
         * @param out The path to the file the entity should be dumped to.
         * @param format The [Format] to export the data.
         * @param client The [SimpleClient] to use.
         */
        fun dumpEntity(entityName: Name.EntityName, out: Path, format: Format, client: SimpleClient){
            val qm = Query(entityName.toString())
            val path = out.resolve("${entityName}.${format.suffix}")
            val dataExporter = format.newExporter(path)

            try {
                val duration = measureTime {
                    /* Execute query and export data. */
                    val results = client.query(qm)
                    for (r in results) {
                        dataExporter.offer(r)
                    }
                    dataExporter.close()
                }
                println("Dumping ${entityName} took $duration.")
            } catch (e: Throwable) {
                print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
            }
        }
    }

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

    override fun exec() = dumpEntity(this.entityName, this.out, this.format, this.client)
}