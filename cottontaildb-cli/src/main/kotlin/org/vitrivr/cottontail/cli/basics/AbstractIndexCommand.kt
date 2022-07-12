package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.core.database.Name

/**
 * An [AbstractCottontailCommand] that concerns the inspection and management of indexes.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractIndexCommand(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
    /** The [Name.EntityName] affected by this [AbstractCottontailCommand.Entity]. */
    protected val indexName: Name.IndexName by argument(name = "index", help = "The fully qualified index name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>.<index>.").convert {
        val split = it.split(Name.DELIMITER)
        when(split.size) {
            1, 2 -> throw IllegalArgumentException("'$it' is not a valid entity name. Entity name must contain schema specified.")
            3 -> Name.IndexName(split[0], split[1], split[2])
            4 -> {
                require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                Name.IndexName(split[1], split[2], split[3])
            }
            else -> throw IllegalArgumentException("'$it' is not a valid entity name.")
        }
    }
}