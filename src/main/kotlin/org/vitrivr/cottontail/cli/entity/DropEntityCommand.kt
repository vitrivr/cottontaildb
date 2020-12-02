package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to drop, i.e., remove a [org.vitrivr.cottontail.database.entity.Entity] by its name.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class DropEntityCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractEntityCommand(name = "drop", help = "Drops the given entity from the database. Usage: entity drop <schema>.<entity>") {

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