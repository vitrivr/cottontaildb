package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.*
import org.vitrivr.cottontail.cli.basics.AbstractSchemaCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.extensions.proto
import java.net.URI
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createDirectory
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime


/**
 * Command to dump the content of all entities within a schema into individual files.
 *
 * @author Florian Spiess
 * @version 2.0.0
 */
@ExperimentalTime
class DumpSchemaCommand(client: SimpleClient) : AbstractSchemaCommand(
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

    /** Flag indicating whether output should be compressed. */
    private val compress: Boolean by option(
        "-c",
        "--compress",
        help = "Whether export should be compressed)"
    ).flag(default = false)


    override fun exec() {

        /* Generate file system. */
        val out = this.out.resolve(this.schemaName.simple)
        val root = if (this.compress) {
            val env = mutableMapOf("create" to "true")
            val uri = URI.create("jar:file:$out.zip")
            val fs = FileSystems.newFileSystem(uri, env)
            fs.getPath("/")
        } else {
            out.createDirectory() /* Create directory. */
            out
        }

        for (entity in this.client.list(CottontailGrpc.ListEntityMessage.newBuilder().setSchema(schemaName.proto()).build())) {
            val entityName = entity.asString(0)
            val qm = Query(entityName)
            val file = root.resolve("$entityName.${this.format.suffix}")
            val dataExporter = this.format.newExporter(file)
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

        /** Close ZIP-file system. */
        if (compress) root.fileSystem.close()
    }
}