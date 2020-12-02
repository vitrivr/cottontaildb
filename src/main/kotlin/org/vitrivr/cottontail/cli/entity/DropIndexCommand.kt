package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to drop an index for a specified entity from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.0
 */
@ExperimentalTime
class DropIndexCommand(
        private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub)
    : AbstractEntityCommand(
        name="drop-index",
        help="Drops the index on an entity. Usage: entity drop-index <schema>.<entity> <index>"
) {

    val name: String by argument(
            name="name", help = "The name of the index. Ideally from a previous list-indices call")

        override fun exec() {
            val indices = ddlStub.listIndexes(entityName.proto()).asSequence()
            if(!indices.map { it.name }.contains(name)){
                println("Entity $entityName does not have such an index $name.")
                return
            }
            val idx = Name.IndexName(*entityName.components + name)
            val status = measureTimedValue {
                ddlStub.dropIndex(idx.proto())
            }
            if(status.value.success){
                println("Successfully dropped index $name (in ${status.duration})")
            }else{
                println("Failed to remove the index")
            }
    }
}