package org.vitrivr.cottontail.cli.schema

import com.jakewharton.picnic.table
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.server.grpc.helper.fqn
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.database.schema.Schema] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class ListAllSchemaCommand(private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractCottontailCommand(name = "all", help = "Lists all schemas stored in Cottontail DB. Usage: schema all") {
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
                    }
                }
                this@ListAllSchemaCommand.ddlStub.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
                    row {
                        val e = _schema.fqn()
                        cell("warren")
                        cell(e.simple)
                    }
                    hits++
                }
            }
        }

        /* Output results. */
        println("$hits schemas found (took ${tbl.duration}).")
        println(tbl.value)
    }
}