package org.vitrivr.cottontail.cli.query

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.vitrivr.cottontail.cli.basics.AbstractQueryCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import kotlin.time.ExperimentalTime

/**
 * Returns all distinct rows in an [org.vitrivr.cottontail.dbms.entity.DefaultEntity].
 *
 * @author Silvan Heller
 * @version 1.0.0
 */
@ExperimentalTime
class DistinctColumnQueryCommand(client: SimpleClient) : AbstractQueryCommand(client, name = "distinct", help = "Returns all distinct entries for a given column") {

    private val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
        val split = it.split(Name.DELIMITER)
        when (split.size) {
            1 -> throw IllegalArgumentException("'$it' is not a valid entity name. Entity name must contain schema specified.")
            2 -> Name.EntityName(split[0], split[1])
            3 -> {
                require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                Name.EntityName(split[1], split[2])
            }
            else -> throw IllegalArgumentException("'$it' is not a valid entity name.")
        }
    }

    val col: String by option( "--column", help = "Column name").required()

    private val countOnly: Boolean by option(
            "--count",
            help = "Only counts the number of distinct elements instead of printing them"
    ).flag(default = false)


    override fun exec() {
        val query = Query(this.entityName.toString()).distinct(col)
        if (this.countOnly) {
            var i = 0
            this.client.query(query).forEach { _ -> i++ }
            println("$i distinct elements in ${entityName.fqn}.$col")
        }
        if (this.toFile) {
            this.executeAndExport(query)
        } else {
            this.executeAndTabulate(query)
        }
    }
}