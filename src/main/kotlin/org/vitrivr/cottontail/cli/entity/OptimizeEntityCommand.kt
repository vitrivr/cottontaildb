package org.vitrivr.cottontail.cli.entity

import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to optimize an entity (i.e. rebuild all its index structures).
 *
 * @author Loris Sauter
 * @version 1.0.1
 */
@ExperimentalTime
class OptimizeEntityCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractEntityCommand(name = "optimize", help = "Optimizes the specified entity, i.e., rebuilds its index structures.") {
    override fun exec() {
        println("Optimizing entity ${this.entityName}. This might take a while...")
        val time = measureTime {
            this.ddlStub.optimize(this.entityName.proto())
        }
        println("Successfully optimized entity ${this.entityName} (took $time).")
    }
}