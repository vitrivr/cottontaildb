package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.predicate.Expression
import org.vitrivr.cottontail.core.database.Name
import kotlin.time.ExperimentalTime

/**
 * Command to filter a given entity, identified by an entity name and a column / value pair.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.1.0
 */
@ExperimentalTime
class FindInEntityCommand(client: SimpleClient): AbstractCottontailCommand.Query(client, name = "find", help = "Find within an entity by column-value specification") {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
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

    /** The name of the column to query. */
    private val column: String by option("-c", "--column", help = "The name of the column to query.").required()

    /** The query value. */
    private val value: String by option("-v", "--value", help = "The query value.").required()

    /** Flag indicating, that the query value should be interpreted as pattern. */
    private val pattern: Boolean by option("-p", "--pattern", "The ").flag("--no-pattern", default = false)

    override fun exec() {

        /* Prepare query. */
        val query = org.vitrivr.cottontail.client.language.dql.Query(this.entityName.toString()).select("*")
        if (this.pattern) {
            query.where(Expression(this.column, "=", this.value))
        } else {
            query.where(Expression(this.column, "LIKE", this.value))
        }


        /* Execute query based on options. */
        if (this.toFile) {
            this.executeAndExport(query)
        } else {
            this.executeAndTabulate(query)
        }
    }
}