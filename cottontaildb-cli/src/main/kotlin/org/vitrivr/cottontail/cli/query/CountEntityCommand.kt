package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import kotlin.time.ExperimentalTime

/**
 * Counts the number of rows in an [org.vitrivr.cottontail.dbms.entity.DefaultEntity].
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.2
 */
@ExperimentalTime
class CountEntityCommand(client: SimpleClient): AbstractCottontailCommand.Query(client, name = "count", help = "Counts the number of entries in the given entity. Usage: entity count <schema>.<entity>") {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        Name.EntityName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
    }

    override fun exec() {
        val query = Query(this.entityName.toString()).count()
        if (this.toFile) {
            this.executeAndExport(query)
        } else {
            this.executeAndTabulate(query)
        }
    }
}