package org.vitrivr.cottontail.cli.index

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.associate
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.CreateIndex
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a index on a specified entities column from Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.1.0
 */
@ExperimentalTime
class CreateIndexCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "create", help = "Creates an index on the given entity and rebuilds the newly created index. Usage: index create <schema>.<entity> <column> <index>") {
    /** The attribute that should be indexed. */
    private val attribute by argument(name = "column", help = "The name of the column to create the index for.")

    /** The type of index to create. */
    private val index by argument(name = "index", help = "The type of index to create.").enum<IndexType>()

    /** Index configuration parameters. */
    private val configuration: Map<String, String> by option("-D", "--config").associate()

    override fun exec() {
        try {
            var create = CreateIndex(this.entityName, this.index).column(this.attribute)
            for ((k,v) in this.configuration) {
                create = create.param(k, v)
            }
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.create(create))
            }
            println("Successfully created index for ${this.entityName.fqn} (took ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Error while creating index ${this.entityName.fqn}: ${e.message}.")
        }
    }
}