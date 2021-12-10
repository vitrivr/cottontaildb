package org.vitrivr.cottontail.cli.system

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.types.long
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Kills a specific transactions
 *
 * @author Ralph Gasser & Loris Sauter
 * @version 2.0.0
 */
@ExperimentalTime
class KillTransactionCommand(private val client: SimpleClient) : AbstractCottontailCommand.System(name = "kill", help = "Kills an ongoing transaction.") {

    private val txid by argument("txid").long()

    override fun exec() {
        /* Execute Rollback */
        val duration = measureTime { this.client.kill(this.txid) }

        /* Output results. */
        println("Killing of transaction $txid completed (took $duration)!")
    }
}
