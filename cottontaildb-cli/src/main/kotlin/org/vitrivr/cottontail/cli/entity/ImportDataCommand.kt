package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.enum
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchInsertMessage
import org.vitrivr.cottontail.utilities.extensions.proto
import org.vitrivr.cottontail.utilities.extensions.protoFrom
import org.vitrivr.cottontail.utilities.extensions.toLiteral
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to import data into a specified entity in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ImportDataCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "import", help = "Used to import data into Cottontail DB.") {

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
        help = "Location of the data to be imported"
    ).convert { Paths.get(it) }.required()

    /** Flag indicating, whether the import should be executed in a single transaction or not. */
    private val singleTransaction: Boolean by option("-t", "--transaction").flag()

    override fun exec() {
        /* Read schema and prepare Iterator. */
        val schema = this.readSchema()
        val iterator = this.format.newImporter(this.input, schema)

        /** Begin transaction (if single transaction option has been set). */
        val txId = if (this.singleTransaction) {
            this.client.begin()
        } else {
            null
        }

        try {
            /* Prepare batch insert message. */
            val batchedInsert = BatchInsertMessage.newBuilder()
            if (txId != null) {
                batchedInsert.metadataBuilder.transactionId = txId
            }
            batchedInsert.from = this.entityName.protoFrom()
            for (c in schema) {
                batchedInsert.addColumns(c.name.proto())
            }
            var count = 0L
            val headerSize = batchedInsert.build().serializedSize
            var cummulativeSize = headerSize
            val duration = measureTime {
                iterator.forEach {
                    val element = BatchInsertMessage.Insert.newBuilder()
                    for (c in schema) {
                        element.addValues(it[c]?.toLiteral())
                    }
                    val built = element.build()
                    if ((cummulativeSize + built.serializedSize) >= Constants.MAX_PAGE_SIZE_BYTES) {
                        this.client.insert(batchedInsert.build())
                        batchedInsert.clearInserts()
                        cummulativeSize = headerSize
                    }
                    cummulativeSize += built.serializedSize
                    batchedInsert.addInserts(built)
                    count+= 1
                }

                /** Insert remainder. */
                if (batchedInsert.insertsCount > 0) {
                    this.client.insert(batchedInsert.build())
                }

                /** Commit transaction, if single transaction option has been set. */
                if (txId != null) {
                    this.client.commit(txId)
                }
            }
            println("Importing $count entries into ${this.entityName} took $duration.")
        } catch (e: Throwable) {
            /** Rollback transaction, if single transaction option has been set. */
            if (txId != null) this.client.rollback(txId)
            println("Importing entries into ${this.entityName} failed due to error: ${e.message}")
        } finally {
            iterator.close()
        }
    }

    /**
     * Reads the column definitions for the specified schema and returns it.
     *
     * @return List of [ColumnDef] for the current [Name.EntityName]
     */
    private fun readSchema(): List<ColumnDef<*>> {
        val columns = mutableListOf<ColumnDef<*>>()
        val schemaInfo = this.client.about(AboutEntity(this.entityName.toString()))
        schemaInfo.forEach {
            if (it.asString(1) == "COLUMN") {
                columns.add(
                    ColumnDef(
                        name = Name.ColumnName(it.asString(0)!!.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray()),
                        type = Types.forName(it.asString(2)!!, it.asInt(4)!!),
                        nullable =  it.asBoolean(5)!!
                    )
                )
            }
        }
        return columns
    }
}