package org.vitrivr.cottontail.cli.schema

import com.jakewharton.picnic.table

import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.fqn
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to list available entities and schemata
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@ExperimentalTime
class ListEntitiesCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractSchemaCommand(name = "entities", help = "Lists all entities for a given schema.") {

    override fun exec() {
        /* Execute query. */
        var hits = 0
        val tbl = measureTimedValue {
            table {
                cellStyle {
                    border = true
                    paddingLeft = 1
                    paddingRight = 1
                }
                header {
                    row {
                        cell("Node")
                        cell("Schema")
                        cell("Entity")
                    }
                }
                this@ListEntitiesCommand.ddlStub.listEntities(this@ListEntitiesCommand.schemaName.proto()).forEach { _entity ->
                    row {
                        val e = _entity.fqn()
                        cell("warren")
                        cell(e.schema().simple)
                        cell(e.simple)
                    }
                    hits++
                }
            }
        }

        /* Output results. */
        println("List entities for ${this.schemaName} found $hits entries (took ${tbl.duration}).}")
        println(tbl.value)
    }
}