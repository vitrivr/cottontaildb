package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to drop, i.e. remove the given entity
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@ExperimentalTime
class DropEntityCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractEntityCommand(name = "drop", help = "Drops the given entity from the database and deletes it therefore") {

    private val confirm by option().prompt(text = "Do you really want to drop the entity ${this.entityName} (y/N)?")

    override fun exec() {
        if (this.confirm.toLowerCase() == "y") {
            val time = measureTime {
                this.ddlStub.dropEntity(this.entityName.proto())
            }
            println("Successfully dropped entity ${this.entityName} (took $time).")
        } else {
            println("Drop entity aborted.")
        }
    }
}