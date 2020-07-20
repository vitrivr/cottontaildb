package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.CliktHelpFormatter
import com.github.ajalt.clikt.output.HelpFormatter
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.long
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import kotlin.system.exitProcess

/**
 * Wrapper and single access to the actual commands.
 * Also, provides the GRPC bindings
 *
 * How to add more commands:
 * Write dedicated inner class (extend [AbstractEntityCommand] if it's an entity specific command, otherwise [AbstractCottontailCommand]
 * Add this command to subcommands call in line 32ff (init of this class)
 */
class CottontailCommand(private val host:String, private val port:Int):NoOpCliktCommand("cottontaildb"){
    init {
        context { helpFormatter = CliHelpFormatter() }
        subcommands(
                ListAllEntitiesCommand(),
                PreviewEntityCommand(),
                ShowEntityCommand(),
                DropEntityCommand(),
                OptimizeEntityCommand(),
                CountEntityCommand(),
                QueryByColumnValueEqualsEntityCommand(),
                StopCommand())
    }

    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()

    val dqlService = CottonDQLGrpc.newBlockingStub(channel)
    val ddlService = CottonDDLGrpc.newBlockingStub(channel)
    val dmlService = CottonDMLGrpc.newBlockingStub(channel)

    /**
     * Base class for none entity specific commands (for potential future generalisation)
     */
    abstract inner class AbstractCottontailCommand(name:String, help:String): CliktCommand(name=name, help=help){

    }

    /**
     * Base class for entity specific commands, providing / querying from user schema and entity as arguments
     */
    abstract inner class AbstractEntityCommand(name:String, help:String):CliktCommand(name=name, help=help){
        protected val schema:String by option("-s", "--schema", help="The schema name this command targets at").required()
        protected val entity:String by option("-e", "--entity", help="The entity name this command targets at").required()
    }

    /**
     * Command to list available entities and schemata
     */
    inner class ListAllEntitiesCommand() :AbstractCottontailCommand(name="list", help="Lists all entities in all schemata"){
        override fun run() {
            this@CottontailCommand.ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
                println("Entities for Schema ${_schema.name}:")
                this@CottontailCommand.ddlService.listEntities(_schema).forEach { _entity ->
                    if (_entity.schema.name != _schema.name) {
                        println("Data integrity threat! entity $_entity ist returned when listing entities for schema $_schema")
                    }
//                    println("${_schema.name} - ${_entity.name}")
                    println("  ${_entity.name}")
                }
            }
        }
    }

    /**
     * Command to preview a given entity
     */
    inner class PreviewEntityCommand(): AbstractEntityCommand(name="preview", help="Gives a preview of the entity specified"){
        private val limit : Long by option("-l", "--limit", help="Limits the amount of printed results").long().default(10).validate { require(it > 0) }
        override fun run() {
            println("Showing first $limit elements of entity $schema.$entity")
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(From(Entity(entity, Schema(schema))))
                            .setProjection(MatchAll())
                            .setLimit(limit)
            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            println("Previewing $limit elements of $entity")
            query.forEach { page ->
                println(page.resultsList.dropLast(Math.max(0, page.resultsCount - limit).toInt()))
            }
        }

    }

    /**
     * COmmand for entity details
     */
    inner class ShowEntityCommand(): AbstractEntityCommand(name="show", help="Gives an overview of the entity and its columns") {
        override fun run() {
            val details = this@CottontailCommand.ddlService.entityDetails(Entity(entity, Schema(schema)))
            println("Entity ${details.entity.schema.name}.${details.entity.name} with ${details.columnsCount} columns: ")
            print("  ${details.columnsList.map { "${it.name} (${it.type})" }.joinToString(", ")}")
        }
    }

    /**
     * Command to optimize an entity
     */
    inner class OptimizeEntityCommand(): AbstractEntityCommand(name="optimize", help="Optimizes the specified entity, e.g. rebuilds the indices"){
        override fun run() {
            println("Optimizing entity $schema.$entity")
            this@CottontailCommand.ddlService.optimizeEntity(Entity(entity, Schema(schema)))
        }
    }

    inner class CountEntityCommand(): AbstractEntityCommand(name="count", help="Counts the given entity's rows"){
        override fun run() {
            println("Counting elements of entity $schema.$entity")
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(From(Entity(entity, Schema(schema))))
                            .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.COUNT).build())

            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            query.forEach { page ->
                if (!page.resultsList.isEmpty()) {
                    println(page.resultsList.first())
                }
            }
        }
    }

    inner class QueryByColumnValueEqualsEntityCommand():AbstractEntityCommand(name="find", help="Find within an entity by column-value specification"){
        val col: String by option("-c", "--column", help="Column name").required()
        val value: String by option("-v", "--value", help="The value").required()
        override fun run() {
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(From(Entity(entity, Schema(schema))))
                            .setProjection(MatchAll())
                            .setWhere(Where(
                                    CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                            .setAttribute(col)
                                            .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                            .addData(CottontailGrpc.Data.newBuilder().setStringData(value).build())
                                            .build()
                            ))
            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            var size = 0
            query.forEach { page ->
                println(page.resultsList)
                size += page.resultsCount
            }
            println("Printed $size individual results")
        }
    }

    /**
     * Command to drop, i.e. remove the given entity
     */
    inner class DropEntityCommand(): AbstractEntityCommand(name="drop", help="Drops the given entity from the database and deletes it therefore"){
        override fun run() {
            println("Dropping entity $schema.$entity")
            this@CottontailCommand.ddlService.dropEntity(Entity(entity, Schema(schema)))
        }
    }

    /**
     * Dedicated help formatter
     */
    inner class CliHelpFormatter : CliktHelpFormatter() {
        override fun formatHelp(
                prolog: String,
                epilog: String,
                parameters: List<HelpFormatter.ParameterHelp>,
                programName: String): String = buildString {
            addOptions(parameters)
            addArguments(parameters)
            addCommands(parameters)
        }
    }

    /**
     * Stops the entire application
     */
    inner class StopCommand() : AbstractCottontailCommand(name="stop", help="Stops the database server and this CLI"){
        override fun run() {
            println("Stopping now...")
            this@CottontailCommand.channel.shutdown()
            Cli.stopServer()
            exitProcess(0)
        }

    }

}