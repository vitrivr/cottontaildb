package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.jakewharton.picnic.table
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.ListEntities
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.TabulationUtilities
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a new a [org.vitrivr.cottontail.dbms.entity.DefaultEntity].
 *
 * @author Ralph Gasser
 * @version 2.0.1
 */
@ExperimentalTime
class CreateEntityCommand(client: SimpleClient) : AbstractEntityCommand(client, name = "create", help = "Creates a new entity in the database. Usage: entity create <schema>.<entity>") {
    /** A [List] of [ColumnDef] objects parsed directly from stdin. */
    private val columnDefinition by option(
        "-d", "--definition",
        help = "List of column definitions in the form of {\"name\": ..., \"type\": ..., \"nullable\": ..., \"size\": ...}"
    ).convert { string ->
        Json.decodeFromString<List<ColumnDef<*>>>(string)
    }

    override fun exec() {
        /* Perform sanity check. */
        checkValid()

        /* Prepare entity definition and prompt user use to add columns to definition. */
        val createMessage = CreateEntity(this.entityName)
        if (this.columnDefinition != null && this.columnDefinition!!.isNotEmpty()) {
            this.columnDefinition!!.forEach {
                createMessage.column(it)
            }

            val time = measureTimedValue {
                TabulationUtilities.tabulate(this.client.create(createMessage))
            }
            println("Entity ${this.entityName} created successfully (took ${time.duration}).")
            print(time.value)
        } else {

            val columns = LinkedList<ColumnDef<*>>()
            do {
                /* Prompt user for column definition. */
                println("Please specify column to add at position ${createMessage.columns() + 1}.")
                val column = this.promptForColumn()
                if (column != null) {
                    columns.add(column)
                    createMessage.column(column)
                }

                /* Ask if another column should be added. */
                if (createMessage.columns() > 0 && this.confirm("Do you want to add another column (size = ${createMessage.columns()})?") == false) {
                    break
                }
            } while (true)



            /* Prepare table for printing. */
            val tbl = table {
                cellStyle {
                    border = true
                    paddingLeft = 1
                    paddingRight = 1
                }
                header {
                    row {
                        cell(this@CreateEntityCommand.entityName) {
                            columnSpan = 4
                        }
                    }
                    row {
                        cells("Name", "Type", "Size", "Nullable")
                    }
                }
                body {
                    columns.forEach { def ->
                        row {
                            cells(def.name, def.type.name, def.type.logicalSize, def.nullable)
                        }
                    }
                }
            }

            /* As for final confirmation and create entity. */
            if (this.confirm(text = "Please confirm that you want to create the entity:\n$tbl", default = true) == true) {
                val time = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.create(createMessage))
                }
                println("Entity ${this.entityName} created successfully (took ${time.duration}).")
                print(time.value)
            } else {
                println("Create entity ${this.entityName} aborted!")
            }

        }
    }

    /**
     * Checks if this [Name.EntityName] is actually valid to create an [Entity] for.
     *
     * @return true if name is valid, false otherwise.
     */
    private fun checkValid(): Boolean {
        try {
            val ret = this.client.list(ListEntities(this.entityName.schema().toString()))
            while (ret.hasNext()) {
                if (ret.next().asString(0) == this.entityName.simple) {
                    println("Error: Entity ${this.entityName} already exists!")
                    return false
                }
            }
            return true
        } catch (e: StatusRuntimeException) {
            if (e.status == Status.NOT_FOUND) {
                println("Error: Schema ${this.entityName.schema()} not found.")
            }
            return false
        }
    }

    /**
     * Prompts user to specify column information and returns a [ColumnDef] if user completes this step.
     *
     * @return Optional [ColumnDef
     */
    private fun promptForColumn(): ColumnDef<*>? {
        val name = this.terminal.prompt("Column name (must consist of letters and numbers)") ?: return null
        val typeName = this.terminal.prompt("Column type (${CottontailGrpc.Type.entries.joinToString(", ")})") ?: return null
        val typeSize = when (typeName.uppercase()) {
            "INTEGER_VECTOR",
            "LONG_VECTOR",
            "FLOAT_VECTOR",
            "DOUBLE_VECTOR",
            "BOOL_VECTOR",
            "COMPLEX32_VECTOR",
            "COMPLEX64_VECTOR" -> this.terminal.prompt("\rColumn lengths (i.e., number of entries for vectors)")?.toIntOrNull() ?: 1
            else -> -1
        }
        val nullable = this.confirm(text = "Should column be nullable?", default = false) ?: false
        val def = ColumnDef(Name.ColumnName.parse(name), Types.forName(typeName, typeSize), nullable)
        val tbl = table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }
            header {
                row {
                    cells("Name", "Type", "Size", "Nullable")
                }
            }
            body {
                row {
                    cells(def.name, def.type.name, def.type.logicalSize, def.nullable)
                }
            }
        }
        return if (this.confirm("Please confirm column:\n$tbl") == true) {
            def
        } else {
            null
        }
    }
}