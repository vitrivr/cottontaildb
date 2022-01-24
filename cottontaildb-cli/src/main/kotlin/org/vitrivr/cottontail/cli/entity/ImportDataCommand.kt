package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.enum
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.utilities.extensions.protoFrom
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime

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
        help = "Limits the amount of printed results"
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
            /* Perform insert. */
            iterator.forEach {
                it.from = this.entityName.protoFrom()
                if (txId != null) {
                    it.metadataBuilder.transactionId = txId
                }
                this.client.insert(it.build())
            }

            /** Commit transaction, if single transaction option has been set. */
            if (txId != null) {
                this.client.commit(txId)
            }
        } catch (e: Throwable) {
            /** Rollback transaction, if single transaction option has been set. */
            if (txId != null) {
                this.client.rollback(txId)
            }
        } finally {
            iterator.close()
        }
    }

    /**
     * Reads the column definitions for the specified schema and returns it.
     *
     * @return List of [ColumnDef] for the current [Name.EntityName]
     */
    private fun readSchema(): Array<ColumnDef<*>> {
        val columns = mutableListOf<ColumnDef<*>>()
        val schemaInfo = this.client.about(AboutEntity(this.entityName.toString()))
        schemaInfo.forEach {
            if (it.asString(1) == "COLUMN") {
                columns.add(
                    ColumnDef(
                        name = Name.ColumnName(*it.asString(0)!!.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray()),
                        type = Types.forName(it.asString(2)!!, it.asInt(4)!!),
                        nullable =  it.asBoolean(5)!!
                    )
                )
            }
        }
        return columns.toTypedArray()
    }
}