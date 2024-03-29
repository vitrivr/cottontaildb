package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.mordant.terminal.YesNoPrompt
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.cli.schema.DropSchemaCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.predicate.Compare
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to remove a row from an [org.vitrivr.cottontail.dbms.entity.DefaultEntity] by its value in a specific column.
 *
 * @author Silvan Heller
 * @version 1.0.2
 */
@ExperimentalTime
class DeleteRowCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "delete-row", help = "Deletes all rows where a given column matches a given value from the database. Usage: entity delete-row -c <col> -v <value> <schema>.<entity>") {
    /** The column to use for the comparison. */
    val column: String by option("-c", "--column", help = "Column name").required()

    /** The value to use for the comparison. */
    val value: String by option("-v", "--value", help = "The value").required()

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

    /** The [YesNoPrompt] used by the [DropSchemaCommand] */
    private val prompt = YesNoPrompt("Do you really want to delete the rows from ${this.entityName} where where $column = $value [y/N]?", this.terminal, default = false)

    override fun exec() {
        if (this.confirm || this.prompt.ask() == true) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.delete(Delete(this.entityName).where(Compare(this.column, "=", this.value))))
                }
                println("Successfully deleted rows where $column = $value from entity ${this.entityName} (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to delete rows from entity ${this.entityName} due to error: ${e.message}.")
            }
        } else {
            println("Delete row aborted.")
        }
    }
}