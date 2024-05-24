package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.enum
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.csv.Csv
import kotlinx.serialization.json.DecodeSequenceMode
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeToSequence
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.serialization.kotlinx.descriptionSerializer
import org.vitrivr.cottontail.serialization.kotlinx.serializer
import java.nio.file.Files
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

    /** The [Format] used for the import. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Format for used for data import (Options: ${
            Format.entries.joinToString(", ")
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
    override fun exec() {
        /* Read schema and prepare Iterator. */
        val schema = this.client.readSchema(this.entityName).toTypedArray()
        Files.newInputStream(this.input).use { input ->
            val data: Sequence<Tuple> = when(format) {
                Format.CBOR -> Cbor.decodeFromByteArray(ListSerializer(schema.serializer()), input.readAllBytes()).asSequence()
                Format.JSON -> Json.decodeToSequence(input, schema.serializer(), DecodeSequenceMode.ARRAY_WRAPPED)
                Format.CSV -> Csv.decodeFromString(ListSerializer(schema.descriptionSerializer()), input.readAllBytes().toString()).asSequence()
            }

            /** Begin transaction (if single transaction option has been set). */
            val txId = if (this.singleTransaction) {
                this.client.begin()
            } else {
                null
            }

            try {
                /* Prepare batch insert message. */
                val batchedInsert = BatchInsert(this.entityName.fqn)
                if (txId != null) {
                    batchedInsert.txId(txId)
                }
                batchedInsert.columns(*schema.map { it.name.simple }.toTypedArray())
                var count = 0L
                val duration = measureTime {
                    for (t in data) {
                        if (!batchedInsert.values(*t.values().mapNotNull { it as? PublicValue }.toTypedArray())) {
                            /* Execute insert... */
                            client.insert(batchedInsert)

                            /* ... now clear and append. */
                            batchedInsert.clear()
                            if (!batchedInsert.any(*t.values().mapNotNull { it as? PublicValue }.toTypedArray())) {
                                throw IllegalArgumentException("The appended data is too large for a single message.")
                            }
                        }
                        count += 1
                    }

                    /** Insert remainder. */
                    if (batchedInsert.count() > 0) {
                        this.client.insert(batchedInsert)
                    }

                    /** Commit transaction, if single transaction option has been set. */
                    if (txId != null) {
                        this.client.commit(txId)
                    }
                }
                println("Importing $count entries into $entityName took $duration.")
            } catch (e: Throwable) {
                /** Rollback transaction, if single transaction option has been set. */
                if (txId != null) this.client.rollback(txId)
                println("Importing entries into $entityName failed due to error: ${e.message}")
            }
        }
    }
}