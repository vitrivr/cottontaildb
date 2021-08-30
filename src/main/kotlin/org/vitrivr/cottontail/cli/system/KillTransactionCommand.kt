package org.vitrivr.cottontail.cli.system

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Kills a specific transactions
 *
 * @author Ralph Gasser & Loris Sauter
 * @version 1.0.0
 */
@ExperimentalTime
class KillTransactionCommand(private val txnStub: TXNGrpc.TXNBlockingStub) : AbstractCottontailCommand(name = "kill", help = "Kills an ongoing transaction.") {

    private val txid by argument("TXID").convert {
        CottontailGrpc.TransactionId.newBuilder().setValue(it.toLong()).build()
    }

    override fun exec() {
        /* Execute Rollback */
        val duration = measureTime { this.txnStub.kill(this.txid) }

        /* Output results. */
        println("Killing of transaction ${txid.value} completed (took $duration)!")
    }
}
