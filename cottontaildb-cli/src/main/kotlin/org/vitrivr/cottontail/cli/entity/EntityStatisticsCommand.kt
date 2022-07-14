package org.vitrivr.cottontail.cli.entity

import io.grpc.StatusException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.EntityStatistics
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class EntityStatisticsCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "statistics", help = "Lists statistics of an entity and its columns.") {
    override fun exec() {
        try {
            val timedTable = measureTimedValue {
                TabulationUtilities.tabulate(this.client.about(EntityStatistics(this.entityName.toString())))
            }
            println("Statistics for entity ${this.entityName} (took ${timedTable.duration}):")
            print(timedTable.value)
        } catch (e: StatusException) {
            println("Failed to load statistics for entity ${this.entityName}: ${e.message}.")
        }
    }
}