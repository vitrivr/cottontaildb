package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.client.language.basics.predicate.Predicate
import org.vitrivr.cottontail.client.language.ddl.DropEntity
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop, i.e., remove a [org.vitrivr.cottontail.dbms.entity.DefaultEntity] by its name.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class DropRowCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "drop-row", help = "Drops all rows where a given column matches a given value from the database. Usage: entity drop-row -c <col> -v <value> <schema>.<entity>") {

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

    val col: String by option("-c", "--column", help = "Column name").required()
    val value: String by option("-v", "--value", help = "The value").required()

    override fun exec() {
        if (this.confirm || TermUi.confirm("Do you really want to drop the rows from ${this.entityName} where where $col = $value [y/N]?", default = false, showDefault = false) == true) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.delete(Delete().from(entityName.fqn).where(Expression(this.col, "=", this.value))))
                }
                println("Successfully dropped rows where $col = $value from entity ${this.entityName} (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to drop rows from entity ${this.entityName} due to error: ${e.message}.")
            }
        } else {
            println("Drop row aborted.")
        }
    }
}