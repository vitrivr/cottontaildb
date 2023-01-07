package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to remove a row from an [org.vitrivr.cottontail.dbms.entity.DefaultEntity] by its value in a specific column.
 *
 * @author Silvan Heller
 * @version 1.0.0
 */
@ExperimentalTime
class DeleteRowCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "delete-row", help = "Deletes all rows where a given column matches a given value from the database. Usage: entity delete-row -c <col> -v <value> <schema>.<entity>") {

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

    val col: String by option("-c", "--column", help = "Column name").required()
    val value: String by option("-v", "--value", help = "The value").required()

    override fun exec() {
        if (this.confirm || confirm("Do you really want to delete the rows from ${this.entityName} where where $col = $value [y/N]?", default = false, showDefault = false) == true) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.delete(Delete().from(entityName.fqn).where(Expression(this.col, "=", this.value))))
                }
                println("Successfully deleted rows where $col = $value from entity ${this.entityName} (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to delete rows from entity ${this.entityName} due to error: ${e.message}.")
            }
        } else {
            println("Delete row aborted.")
        }
    }
}
