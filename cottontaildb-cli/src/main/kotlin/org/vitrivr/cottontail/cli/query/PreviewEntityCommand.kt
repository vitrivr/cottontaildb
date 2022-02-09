package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.long
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import kotlin.time.ExperimentalTime

/**
 * Command to preview a given entity, identified by an entity name.
 *
 * @author Loris Sauter
 * @version 2.0.0
 */
@ExperimentalTime
class PreviewEntityCommand(client: SimpleClient): AbstractCottontailCommand.Query(client, name = "preview", help = "Gives a preview of the entity specified") {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        val split = it.split(Name.NAME_COMPONENT_DELIMITER)
        when(split.size) {
            1 -> throw IllegalArgumentException("'$it' is not a valid entity name. Entity name must contain schema specified.")
            2 -> Name.EntityName(split[0], split[1])
            3 -> Name.EntityName(split[0], split[1], split[2])
            else -> throw IllegalArgumentException("'$it' is not a valid entity name.")
        }
    }

    /**
     * Upper limit of results. Option given via CLI. Defaults to 50
     */
    private val limit: Long by option("-l", "--limit", help = "Limits the amount of printed results").long().default(50).validate { require(it > 1) }

    /**
     * Offset from the start of the table. Option given vai CLI. Defaults to 0
     */
    private val skip: Long by option("-s", "--skip", help = "The offset on how many rows should be skipped").long().default(0).validate { require(it >= 0) }

    override fun exec() {
        /* Prepare query. */
        val query = Query(this.entityName.toString()).select("*").limit(this.limit).skip(this.skip)
        if (this.toFile) {
            this.executeAndExport(query)
        } else {
            this.executeAndTabulate(query)
        }
    }
}
