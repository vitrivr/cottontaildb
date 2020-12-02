package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.enum
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Command to create a index on a specified entities column from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.0
 */
@ExperimentalTime
class CreateIndexCommand(
        private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub)
    : AbstractEntityCommand(
        name = "create-index",
        help = "Creates an index on the given entity and rebuilds the newly created index. Usage: entity createIndex <schema>.<entity> <column> <index>") {

    private val attribute by argument(name="column", help="The column name to create the index for")
    private val index by argument(name="index", help = "The index to create").enum<CottontailGrpc.IndexType>()

    override fun exec() {
        val entity = entityName.proto()
        val details = this.ddlStub.entityDetails(this.entityName.proto())
        if(!details.columnsList.map { it.name }.contains(attribute)){
            println("The given entity $entity does not have such a column $attribute")
            return
        }

        val idx = CottontailGrpc.Index.newBuilder()
                .setEntity(entity)
                .setName("index-${index.name.toLowerCase()}-${entityName.schema()}_${entity.name}_${attribute}")
                .setType(index)
                .build()
        val idxMsg = CottontailGrpc.IndexDefinition.newBuilder()
                .setIndex(idx).addColumns(attribute).build()
        val status = measureTimedValue {
            ddlStub.createIndex(idxMsg)
        }
        val rebuilt = measureTimedValue {
            ddlStub.rebuildIndex(idx);
        }
        if(status.value.success){
            println("Successfully created index (in ${status.duration}) and rebuilt in ${rebuilt.duration}")
        }else{
            println("Failed to create the index")
        }

    }
}