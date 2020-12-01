package org.vitrivr.cottontail.cli.schema

import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to create a [org.vitrivr.cottontail.database.schema.Schema] from Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class CreateSchemaCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractSchemaCommand(name = "create", help = "Create the schema with the given name. Usage: schema create <name>") {
    override fun exec() {
        /* Execute query. */
        val time = measureTime {
            this.ddlStub.createSchema(this.schemaName.proto())
        }

        /* Output results. */
        println("Schema ${this.schemaName} created successfully (took $time).")
    }
}