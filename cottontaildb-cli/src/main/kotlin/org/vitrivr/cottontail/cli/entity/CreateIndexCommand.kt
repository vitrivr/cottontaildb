package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import io.grpc.StatusException
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
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
 * @version 2.0.1
 */
@ExperimentalTime
class CreateIndexCommand(client: SimpleClient) : AbstractCottontailCommand.Entity(client, name = "create-index", help = "Creates an index on the given entity and rebuilds the newly created index. Usage: entity createIndex <schema>.<entity> <column> <index>") {

    private val attribute by argument(
        name = "column",
        help = "The name of the column to create the index for."
    )
    private val index by argument(
        name = "index",
        help = "The type of index to create."
    ).enum<IndexType>()

    private val skipBuild: Boolean by option(
        "-s",
        "--skip-build",
        help = "Skips the build step for the newly created index. Such an index cannot be used!"
    ).flag(default = false)

    override fun exec() {
        try {
            val create = CreateIndex(this.entityName.fqn, this.attribute, this.index)
            if (!this.skipBuild) {
                create.rebuild()
            }
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.create(create.rebuild()))
            }
            println("Successfully created index for ${this.entityName.fqn} (took ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Error while creating index ${this.entityName.fqn}: ${e.message}.")
        }
    }
}