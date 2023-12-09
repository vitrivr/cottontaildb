package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.core.database.Name

/**
 * An [AbstractCottontailCommand] that concerns the inspection and management of entities.
 *
 * @author Ralph Gasser & Loris Sauter
 * @version 1.0.0
 */
abstract class AbstractEntityCommand(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
    /** The [Name.EntityName] affected by this [AbstractCottontailCommand.Entity]. */
    protected val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        val split = it.split(Name.DELIMITER)
        when(split.size) {
            1 -> throw IllegalArgumentException("'$it' is not a valid entity name. Entity name must contain schema specified.")
            2 -> Name.EntityName.create(split[0], split[1])
            3 -> {
                require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                Name.EntityName.create(split[1], split[2])
            }
            else -> throw IllegalArgumentException("'$it' is not a valid entity name.")
        }
    }
}