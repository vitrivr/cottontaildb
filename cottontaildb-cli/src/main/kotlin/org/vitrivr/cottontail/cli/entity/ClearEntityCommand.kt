package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to clear [org.vitrivr.cottontail.dbms.entity.DefaultEntity], i.e., delete all data from it
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ClearEntityCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "clear", help = "Clears the given entity, i.e., deletes all data it contains. Usage: entity clear <schema>.<entity>") {
    override fun exec() {
        if (TermUi.confirm("Do you really want to delete all data from entity ${this.entityName} (y/N)?", default = false, showDefault = false) == true) {
            val time = measureTimedValue {
                this.client.delete(Delete(this.entityName.toString()))
            }
            println("Successfully cleared entity ${this.entityName} (took ${time.duration}).")
            print(TabulationUtilities.tabulate(time.value))
        } else {
            println("Clear entity aborted.")
        }
    }
}