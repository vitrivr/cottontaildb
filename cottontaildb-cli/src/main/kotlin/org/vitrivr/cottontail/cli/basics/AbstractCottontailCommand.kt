package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.core.CliktCommand
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types

/**
 * Base class for none entity specific commands (for potential future generalisation)

 * @author Ralph Gasser & Loris Sauter
 * @version 3.0.1
 */
abstract class AbstractCottontailCommand(name: String, help: String, val expand: Boolean) : CliktCommand(name = name, help = help) {

    /**
     * The actual command execution. Override this for your command
     */
    abstract fun exec()

    /**
     * Runs the command by calling [AbstractCottontailCommand.exec] safely. Exceptions are caught and printed.
     */
    override fun run() = try {
        exec()
    } catch (e: StatusRuntimeException) {
        println("Command execution failed: ${e.message}")
    }

    companion object {
        /**
         * Reads the column definitions for the specified schema and returns it.
         *
         * @param entityName The [Name.EntityName] of the entity to read.
         * @return List of [ColumnDef] for the current [Name.EntityName]
         */
        fun SimpleClient.readSchema(entityName: Name.EntityName): List<ColumnDef<*>> {
            val columns = mutableListOf<ColumnDef<*>>()
            val schemaInfo = this.about(AboutEntity(entityName.toString()))
            schemaInfo.forEach {
                if (it.asString(1) == "COLUMN") {
                    val name = Name.ColumnName.parse(it.asString(0)!!)
                    columns.add(ColumnDef(name = name, type = Types.forName(it.asString(2)!!, it.asInt(4)!!), nullable =  it.asBoolean(5)!!))
                }
            }
            return columns
        }
    }
}