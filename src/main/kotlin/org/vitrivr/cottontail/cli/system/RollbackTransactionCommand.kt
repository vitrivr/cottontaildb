package org.vitrivr.cottontail.cli.system

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Rollback specific transactions
 *
 * @author Ralph Gasser & Loris Sauter
 * @version 1.0
 */
@ExperimentalTime
class RollbackTransactionCommand(
    private val txnStub: TXNGrpc.TXNBlockingStub
) : AbstractCottontailCommand(name = "rollback", help = "Rollback of a certain transaction") {

    private val txid by argument("TXID").convert {
        CottontailGrpc.TransactionId.newBuilder().setValue(it.toLong()).build()
    }

    override fun exec() {
        /* Execute Rollback */
        val timedTable = measureTimedValue {
            txnStub.rollback(txid)
        }

        /* Output results. */
        println("Rollback of transaction ${txid.value} complete (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}
