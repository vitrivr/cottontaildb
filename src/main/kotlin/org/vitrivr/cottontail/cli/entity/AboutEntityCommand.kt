package org.vitrivr.cottontail.cli.entity

import com.jakewharton.picnic.table
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command for accessing and reviewing [org.vitrivr.cottontail.database.entity.Entity] details.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class AboutEntityCommand(private val dqlStub: CottonDDLGrpc.CottonDDLBlockingStub) : AbstractEntityCommand(name = "about", help = "Gives an overview of the entity and its columns.") {
    override fun exec() {
        val details = measureTimedValue {
            this.dqlStub.entityDetails(this.entityName.proto())
        }
        val tbl = table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }
            header {
                row {
                    cell("Column name")
                    cell("Storage engine")
                    cell("Column type")
                    cell("Column size")
                    cell("Nullable")
                }
            }
            body {
                details.value.columnsList.forEach {
                    row {
                        cell(it.name)
                        cell(it.engine.name)
                        cell(it.type.name)
                        cell(it.length)
                        cell(it.nullable)
                    }
                }
            }
        }


        println("Details for entity ${this.entityName} (took ${details.duration}):")
        println(tbl)
    }
}