package org.vitrivr.cottontail.cli.entity

import com.jakewharton.picnic.table

import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.server.grpc.helper.fqn

import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.database.entity.Entity] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ListAllEntitiesCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractCottontailCommand(name = "all", help = "Lists all entities stored in Cottontail DB.") {
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
                this@ListAllEntitiesCommand.ddlStub.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
                    this@ListAllEntitiesCommand.ddlStub.listEntities(_schema).forEach { _entity ->
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
        }

        /* Output results. */
        println("$hits entities found (took ${tbl.duration}).}")
        println(tbl.value)
    }
}