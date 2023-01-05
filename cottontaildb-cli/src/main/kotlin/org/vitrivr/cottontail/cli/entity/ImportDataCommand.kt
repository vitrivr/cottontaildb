package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.enum
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
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

    companion object {
        /**
         * Imports the content of a file into the specified entity. This method has been separated from the [AboutEntityCommand] instance for testability.
         *
         * @param entityName The [Name.EntityName] of the entity to import into.
         * @param input The path to the file that contains the data.
         * @param format The [Format] of the data to import.
         * @param client The [SimpleClient] to use.
         * @param singleTransaction Flag indicating whether import should happen in a single transaction.
         * @param bestEffort Whether to import "best effort": CSV import is capable of e.g. ignoring broken rows which this flag enables
         */
        fun importData(entityName: Name.EntityName, input: Path, format: Format, client: SimpleClient, singleTransaction: Boolean, bestEffort: Boolean) {
            /* Read schema and prepare Iterator. */
            val schema = readSchema(entityName, client)
            val iterator = format.newImporter(input, schema, bestEffort)

            /** Begin transaction (if single transaction option has been set). */
            val txId = if (singleTransaction) {
                client.begin()
            } else {
                null
            }

            try {
                /* Prepare batch insert message. */
                val batchedInsert = BatchInsertMessage.newBuilder()
                if (txId != null) {
                    batchedInsert.metadataBuilder.transactionId = txId
                }
                batchedInsert.from = entityName.protoFrom()
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
                            val literal = it[c]?.toLiteral()
                            if (literal != null) {
                                element.addValues(literal)
                            } else {
                                element.addValues(CottontailGrpc.Literal.newBuilder().build())
                            }
                        }
                        val built = element.build()
                        if ((cummulativeSize + built.serializedSize) >= Constants.MAX_PAGE_SIZE_BYTES) {
                            client.insert(batchedInsert.build())
                            batchedInsert.clearInserts()
                            cummulativeSize = headerSize
                        }
                        cummulativeSize += built.serializedSize
                        batchedInsert.addInserts(built)
                        count += 1
                    }

                    /** Insert remainder. */
                    if (batchedInsert.insertsCount > 0) {
                        client.insert(batchedInsert.build())
                    }

                    /** Commit transaction, if single transaction option has been set. */
                    if (txId != null) {
                        client.commit(txId)
                    }
                }
                println("Importing $count entries into ${entityName} took $duration.")
            } catch (e: Throwable) {
                /** Rollback transaction, if single transaction option has been set. */
                if (txId != null) client.rollback(txId)
                println("Importing entries into ${entityName} failed due to error: ${e.message}")
            } finally {
                iterator.close()
            }
        }

        /**
         * Reads the column definitions for the specified schema and returns it.
         *
         * @param entityName The [Name.EntityName] of the entity to read.
         * @param client The [SimpleClient] to use.
         * @return List of [ColumnDef] for the current [Name.EntityName]
         */
        private fun readSchema(entityName: Name.EntityName, client: SimpleClient): List<ColumnDef<*>> {
            val columns = mutableListOf<ColumnDef<*>>()
            val schemaInfo = client.about(AboutEntity(entityName.toString()))
            schemaInfo.forEach {
                if (it.asString(1) == "COLUMN") {
                    val split = it.asString(0)!!.split(Name.DELIMITER).toTypedArray()
                    val name = when(split.size) {
                        3 -> Name.ColumnName.create(split[0], split[1], split[2])
                        4 -> {
                            require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                            Name.ColumnName.create(split[1], split[2], split[3])
                        }
                        else -> throw IllegalArgumentException("'$it' is not a valid column name.")
                    }
                    columns.add(ColumnDef(name = name, type = Types.forName(it.asString(2)!!, it.asInt(4)!!), nullable =  it.asBoolean(5)!!))
                }
            }
            return columns
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
        help = "Path of file to be imported"
    ).convert { Paths.get(it) }.required()

    /** Flag indicating, whether the import should be executed in a single transaction or not. */
    private val singleTransaction: Boolean by option("-t", "--transaction", help="Flag indicating, whether the import should be executed in a single transaction or not.").flag()

    /** Flag indicating, whether the import should be fuzzy: Ignore corrupt entries and just continue */
    private val fuzzy: Boolean by option("--fuzzy", help="Flag indicating, whether the import should be fuzzy: Ignore corrupt entries and just continue").flag()

    /**
     * Executes this [ImportDataCommand].
     */
    override fun exec() = importData(this.entityName, this.input, this.format, this.client, this.singleTransaction, this.fuzzy)
}
