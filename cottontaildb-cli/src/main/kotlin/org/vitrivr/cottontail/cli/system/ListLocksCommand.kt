package org.vitrivr.cottontail.cli.system

import org.vitrivr.cottontail.cli.basics.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all known locks on  Cottontail DB resources
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListLocksCommand(private val client: SimpleClient) : AbstractCottontailCommand(name = "locks", help = "Lists all locks in the current Cottontail DB instance.", false) {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.client.locks())
        }

        /* Output results. */
        println("${timedTable.value.rowCount} locks found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}