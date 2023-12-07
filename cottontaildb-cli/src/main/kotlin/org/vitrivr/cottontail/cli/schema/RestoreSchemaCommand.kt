package org.vitrivr.cottontail.cli.schema

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.basics.AbstractSchemaCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.data.Restorer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to restore the content of a database dump into the database.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class RestoreSchemaCommand(client: SimpleClient) : AbstractSchemaCommand(client,
    name = "restore",
    help = "Restores the content of a database dump back into Cottontail DB.",
    false
) {

    /** Path to the output folder or file. */
    private val input: Path by option("-i", "--input", help = "Path to the input folder or file.").convert { Paths.get(it) }.required()

    override fun exec() {
        val restorer = if (Files.isDirectory(this.input)) {
            Restorer.Folder(this.client, this.input, this.schemaName)
        } else {
            Restorer.Zip(this.client, this.input, this.schemaName)
        }
        restorer.use {
            val total = measureTime {
                for (entity in it.manifest.entites) {
                    try {
                        val duration = measureTime {
                            it.restore(entity)
                        }
                        println("Restoring ${this.schemaName.entity(entity.name)} (${entity.size} rows, ${entity.batches} batches) took $duration.")
                    } catch (e: Throwable) {
                        System.err.println("Failed to restore ${this.schemaName.entity(entity.name)} due to error: ${e.message}")
                    }
                }
            }
            println("Completed! Restoring schema ${this@RestoreSchemaCommand.schemaName} took $total.")
        }
    }
}