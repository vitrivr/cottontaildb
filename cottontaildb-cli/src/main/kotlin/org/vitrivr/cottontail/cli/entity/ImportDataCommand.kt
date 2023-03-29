package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.enum
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.data.Format
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to import data into a specified entity in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
@ExperimentalTime
class ImportDataCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "import", help = "Used to import data into Cottontail DB.") {

    companion object {
        /**
         * Imports the content of a file into the specified entity. This method has been separated from the [AboutEntityCommand] instance for testability.
         *
         * @param entityName The [Name.EntityName] of the entity to import into.
         * @param input The path to the file that contains the data.
         * @param format The [Format] of the data to import.
         * @param client The [SimpleClient] to use.
         * @param singleTransaction Flag indicating whether import should happen in a single transaction.
         */
        fun importData(entityName: Name.EntityName, input: Path, format: Format, client: SimpleClient, singleTransaction: Boolean) {
            /* Read schema and prepare Iterator. */
            val schema = client.readSchema(entityName)
            val iterator = format.newImporter(input, schema)

            /** Begin transaction (if single transaction option has been set). */
            val txId = if (singleTransaction) {
                client.begin()
            } else {
                null
            }

            try {
                /* Prepare batch insert message. */
                val batchedInsert = BatchInsert(entityName.fqn)
                if (txId != null) {
                    batchedInsert.txId(txId)
                }
                batchedInsert.columns(*schema.map { it.name.simple }.toTypedArray())
                var count = 0L
                val duration = measureTime {
                    iterator.forEach { next ->
                        val data = schema.map { next[it] }.toTypedArray()
                        if (!batchedInsert.append(*data)) {
                            /* Execute insert... */
                            client.insert(batchedInsert)

                            /* ... now clear and append. */
                            batchedInsert.clear()
                            if (!batchedInsert.append(*data)) {
                                throw IllegalArgumentException("The appended data is too large for a single message.")
                            }
                        }
                        count += 1
                    }

                    /** Insert remainder. */
                    if (batchedInsert.count() > 0) {
                        client.insert(batchedInsert)
                    }

                    /** Commit transaction, if single transaction option has been set. */
                    if (txId != null) {
                        client.commit(txId)
                    }
                }
                println("Importing $count entries into $entityName took $duration.")
            } catch (e: Throwable) {
                /** Rollback transaction, if single transaction option has been set. */
                if (txId != null) client.rollback(txId)
                println("Importing entries into $entityName failed due to error: ${e.message}")
            } finally {
                iterator.close()
            }
        }
    }

    /** The [Format] used for the import. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Format for used for data import (Options: ${
            Format.values().joinToString(", ")
        })."
    ).enum<Format>().required()

    /** The [Path] to the input file. */
    private val input: Path by option(
        "-i",
        "--input",
        help = "The path to the file that contains the data to import."
    ).convert { Paths.get(it) }.required()

    /** Flag indicating, whether the import should be executed in a single transaction or not. */
    private val singleTransaction: Boolean by option("-t", "--transaction").flag()

    /**
     * Executes this [ImportDataCommand].
     */
    override fun exec() = importData(this.entityName, this.input, this.format, this.client, this.singleTransaction)
}