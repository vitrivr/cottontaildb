package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.OptimizeEntity
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to optimize an entity (i.e. rebuild all its index structures).
 *
 * @author Loris Sauter
 * @version 2.0.0
 */
@ExperimentalTime
class OptimizeEntityCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "optimize", help = "Optimizes the specified entity, i.e., rebuilds its index structures.") {
    override fun exec() {
        println("Optimizing entity ${this.entityName}. This might take a while...")
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.optimize(OptimizeEntity(this.entityName.toString())))
            }
            println("Successfully optimized entity ${this.entityName} (took ${timedTable.duration}).")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to optimize entity ${this.entityName}: ${e.message}")
        }
    }
}