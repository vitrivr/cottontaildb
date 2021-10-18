package org.vitrivr.cottontail.cli.system

import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all known transactions in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListTransactionsCommand(private val client: SimpleClient) : AbstractCottontailCommand.System(name = "transactions", help = "Lists all ongoing transaction in the current Cottontail DB instance.") {

    /** Flag that can be used to directly provide confirmation. */
    private val all: Boolean by option("-a", "--all", help = "If this flag is set, all transactions are listed and not just the running ones.").flag()

    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulateIf(this.client.transactions()) {
                this@ListTransactionsCommand.all || (it.getData(2).stringData == TransactionStatus.RUNNING.toString() || it.getData(2).stringData == TransactionStatus.ERROR.toString())
            }
        }

        /* Output results. */
        println("${timedTable.value.rowCount} transactions found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}