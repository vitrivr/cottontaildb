package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.arguments.argument
import com.jakewharton.picnic.table
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.server.grpc.helper.proto
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a index on a specified entities column from Cottontail DB.
 *
 * @author Loris Sauter
 * @version 1.0.0
 */
@ExperimentalTime
class ListIndicesComand(
        private val ddlStub: CottonDDLGrpc.CottonDDLBlockingStub)
    : AbstractEntityCommand(
        name="list-indices",
        help="Lists the indices on an entity"
    ) {

    override fun exec() {
        val time = measureTimedValue {
            ddlStub.listIndexes(entityName.proto())
        }
        val indices = time.value.iterator().asSequence().toList()

        val tbl = table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }
            header {
                row {
                    cell("Index name")
                    cell("Index type")
                    cell("Column name")
                }
            }
            body {
                indices.forEach {
                    row {
                        cell(it.name)
                        cell(it.type.name)
                        cell("Coming Soon(tm)")
                    }
                }
            }
        }

        println("Showing index information of $entityName (took ${time.duration})")
        println(tbl)
    }

}