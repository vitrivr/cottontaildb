package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime

/**
 * Executes a query read from a .proto file.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ExecuteQueryCommand(dqlStub: DQLGrpc.DQLBlockingStub) : AbstractQueryCommand(name = "execute", help = "Counts the number of entries in the given entity. Usage: entity count <schema>.<entity>", stub = dqlStub) {

    /** Path to .proto file that contains query. */
    private val input: Path by option("-i", "--input", help = "Path to input .proto file that contains query.").convert { Paths.get(it) }.required()

    override fun exec() {
        val qm = Files.newInputStream(this.input).use {
            CottontailGrpc.QueryMessage.parseFrom(it)
        }

        /* Execute query based on options. */
        if (this.toFile) {
            this.executeAndExport(qm)
        } else {
            this.executeAndTabulate(qm)
        }
    }
}