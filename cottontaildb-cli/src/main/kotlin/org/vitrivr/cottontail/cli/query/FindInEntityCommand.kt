package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.extensions.Literal
import org.vitrivr.cottontail.core.database.Name
import kotlin.time.ExperimentalTime

/**
 * Command to filter a given entity, identified by an entity name and a column / value pair.
 *
 * @author Loris Sauter
 * @version 2.0.0
 */
@ExperimentalTime
class FindInEntityCommand(client: SimpleClient): AbstractCottontailCommand.Query(client, name = "find", help = "Find within an entity by column-value specification") {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        val split = it.split(Name.NAME_COMPONENT_DELIMITER)
        when(split.size) {
            1 -> throw IllegalArgumentException("'$it' is not a valid entity name. Entity name must contain schema specified.")
            2 -> Name.EntityName(split[0], split[1])
            3 -> Name.EntityName(split[0], split[1], split[2])
            else -> throw IllegalArgumentException("'$it' is not a valid entity name.")
        }
    }

    val col: String by option("-c", "--column", help = "Column name").required()
    val value: String by option("-v", "--value", help = "The value").required()
    override fun exec() {

        val query = org.vitrivr.cottontail.client.language.dql.Query(this.entityName.toString())
            .select("*")
            .where(Literal(this.col,"=", this.value))

        /* Execute query based on options. */
        if (this.toFile) {
            this.executeAndExport(query)
        } else {
            this.executeAndTabulate(query)
        }
    }
}