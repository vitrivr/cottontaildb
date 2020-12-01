package org.vitrivr.cottontail.cli.entity

import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime

/**
 * Command for entity details
 *
 * @author LorisSauter
 * @version 1.0
 */
@ExperimentalTime
class AboutEntityCommand(private val dqlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractEntityCommand(name = "about", help = "Gives an overview of the entity and its columns.") {
    override fun exec() {
        val details = this.dqlStub.entityDetails(this.entityName.proto())
        println("Entity ${details.entity.schema.name}.${details.entity.name} with ${details.columnsCount} columns: ")
        print("  ${details.columnsList.map { "${it.name} (${it.type})" }.joinToString(", ")}")
    }
}