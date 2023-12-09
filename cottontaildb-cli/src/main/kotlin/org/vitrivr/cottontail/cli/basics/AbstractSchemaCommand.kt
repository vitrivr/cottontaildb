package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.core.database.Name
import kotlin.time.ExperimentalTime

/**
 * An [AbstractCottontailCommand] that concerns the inspection and management of schemata.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
@ExperimentalTime
abstract class AbstractSchemaCommand(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
    /** The [Name.SchemaName] affected by this [AbstractSchemaCommand]. */
    protected val schemaName: Name.SchemaName by argument(name = "schema", help = "The schema name targeted by the command. Has the form of [\"warren\"].<schema>.").convert {
        val split = it.split(Name.DELIMITER)
        when(split.size) {
            1 -> Name.SchemaName.create(split[0])
            2 -> {
                require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                Name.SchemaName.create(split[1])
            }
            else -> throw IllegalArgumentException("'$it' is not a valid schema name.")
        }
    }
}