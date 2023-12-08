package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.mordant.terminal.YesNoPrompt
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.DropEntity
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop, i.e., remove a [org.vitrivr.cottontail.dbms.entity.DefaultEntity] by its name.
 *
 * @author Ralph Gasser
 * @version 2.0.2
 */
@ExperimentalTime
class DropEntityCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "drop", help = "Drops the given entity from the database. Usage: entity drop <schema>.<entity>") {

    /** Flag that can be used to directly provide confirmation. */
    private val confirm: Boolean by option(
        "-c",
        "--confirm",
        help = "Directly provides the confirmation option."
    ).flag()

    override fun exec() {
        val prompt = YesNoPrompt("Do you really want to drop the entity ${this.entityName} [y/N]?", this.terminal, default = false)
        if (this.confirm || prompt.ask() == true) {
            try {
                val timedTable = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.drop(DropEntity(this.entityName.toString())))
                }
                println("Successfully dropped entity ${this.entityName} (took ${timedTable.duration}).")
                print(timedTable.value)
            } catch (e: StatusException) {
                println("Failed to drop entity ${this.entityName} due to error: ${e.message}.")
            }
        } else {
            println("Drop entity aborted.")
        }
    }
}