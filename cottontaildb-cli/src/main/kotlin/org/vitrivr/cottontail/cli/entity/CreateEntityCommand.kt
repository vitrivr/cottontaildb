package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.google.gson.Gson
import com.google.gson.JsonParser
import com.google.gson.reflect.TypeToken
import com.jakewharton.picnic.table
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.cli.basics.AbstractEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListEntities
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.TabulationUtilities
import org.vitrivr.cottontail.utilities.extensions.proto
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

    companion object {
        data class ColumnInfo(val name: String, val type: CottontailGrpc.Type, val nullable: Boolean = false, val size: Int = -1) {
            fun toDefinition() : CottontailGrpc.ColumnDefinition {
                val def = CottontailGrpc.ColumnDefinition.newBuilder()
                def.nameBuilder.name = name
                def.type = type
                def.nullable = nullable
                def.length = size
                return def.build()
            }
        }
    }

    private inline fun <reified T> Gson.fromJson(json: String) =
        fromJson<T>(json, object : TypeToken<T>() {}.type)

    private val columnDefinition by option(
        "-d", "--definition",
        help = "List of column definitions in the form of {\"name\": ..., \"type\": ..., \"nullable\": ..., \"size\": ...}"
    ).convert { string ->
        val canonicalFormat = JsonParser.parseString(string).toString()
        val list: List<ColumnInfo> = Gson().fromJson(canonicalFormat)
        list.map { it.toDefinition() }
    }

    override fun exec() {
        /* Perform sanity check. */
        checkValid()

        /* Prepare entity definition and prompt user use to add columns to definition. */
        val colDef = CottontailGrpc.EntityDefinition.newBuilder().setEntity(this.entityName.proto())

        if (columnDefinition != null && columnDefinition!!.isNotEmpty()) {
            columnDefinition!!.forEach {
                colDef.addColumns(it)
            }

            val time = measureTimedValue {
                TabulationUtilities.tabulate(this.client.create(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(colDef).build()))
            }
            println("Entity ${this.entityName} created successfully (took ${time.duration}).")
            print(time.value)

        } else {

            do {
                /* Prompt user for column definition. */
                println("Please specify column to add at position ${colDef.columnsCount + 1}.")
                val ret = this.promptForColumn()
                if (ret != null) {
                    colDef.addColumns(ret)
                }

                /* Ask if another column should be added. */
                if (colDef.columnsCount > 0 && this.confirm("Do you want to add another column (size = ${colDef.columnsCount})?") == false) {
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
                    colDef.columnsList.forEach { def ->
                        row {
                            cells(def.name, def.type, def.length, def.nullable)
                        }
                    }
                }
            }

            /* As for final confirmation and create entity. */
            if (this.confirm(text = "Please confirm that you want to create the entity:\n$tbl", default = true) == true) {
                val time = measureTimedValue {
                    TabulationUtilities.tabulate(this.client.create(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(colDef).build()))
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
     * Prompts user to specify column information and returns a [CottontailGrpc.ColumnDefinition] if user completes this step.
     *
     * @return Optional [CottontailGrpc.ColumnDefinition]
     */
    private fun promptForColumn(): CottontailGrpc.ColumnDefinition? {
        val def = CottontailGrpc.ColumnDefinition.newBuilder()
        def.nameBuilder.name = this.prompt("Column name (must consist of letters and numbers)")
        def.type = this.prompt("Column type (${CottontailGrpc.Type.values().joinToString(", ")})") {
            CottontailGrpc.Type.valueOf(it.uppercase())
        }
        def.length = when (def.type) {
            CottontailGrpc.Type.DOUBLE_VECTOR,
            CottontailGrpc.Type.FLOAT_VECTOR,
            CottontailGrpc.Type.LONG_VECTOR,
            CottontailGrpc.Type.INT_VECTOR,
            CottontailGrpc.Type.BOOL_VECTOR    ,
            CottontailGrpc.Type.COMPLEX32_VECTOR,
            CottontailGrpc.Type.COMPLEX64_VECTOR -> this.prompt("\rColumn lengths (i.e. number of entries for vectors)") { it.toInt() } ?: 1
            else -> -1
        }
        def.nullable = this.confirm(text = "Should column be nullable?", default = false) ?: false

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
                    cells(def.name, def.type, def.length, def.nullable)
                }
            }
        }

        return if (this.confirm("Please confirm column:\n$tbl") == true) {
            def.build()
        } else {
            null
        }
    }
}