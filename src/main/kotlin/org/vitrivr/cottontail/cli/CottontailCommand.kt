package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.output.CliktHelpFormatter
import com.github.ajalt.clikt.output.HelpFormatter
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Name.Companion.NAME_COMPONENT_DELIMITER
import org.vitrivr.cottontail.server.grpc.helper.fqn
import org.vitrivr.cottontail.server.grpc.helper.proto
import org.vitrivr.cottontail.server.grpc.helper.protoFrom
import kotlin.time.ExperimentalTime

/**
 * Wrapper class and single access point to the actual commands.  Also, provides the gRPC bindings
 *
 * <strong>How to add more commands:</strong>
 * <ul>
 *     <li>Write dedicated inner class (extend [AbstractEntityCommand] if it's an entity specific command, otherwise [AbstractCottontailCommand])</li>
 *     <li>Add this command to subcommands call in line 32ff (init of this class)</li>
 * </ul>
 *
 * @author Loris Sauter
 * @version 1.0
 */
class CottontailCommand(private val host: String, private val port: Int) : NoOpCliktCommand(name = "cottontail", help = "The base command") {

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
                ReloadCommand(),
                StopCommand()
        )

        initDbCon(host, port)
    }

    lateinit var channel: ManagedChannel
    lateinit var dqlService: CottonDQLGrpc.CottonDQLBlockingStub
    lateinit var ddlService: CottonDDLGrpc.CottonDDLBlockingStub
    lateinit var dmlService: CottonDMLGrpc.CottonDMLBlockingStub


    /**
     * Initialises the gRPC connection to database.
     */
    private fun initDbCon(host: String, port: Int) {
        if (::channel.isInitialized) {
            // Its a reload. Close all
            channel.shutdownNow()
        }
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
        dqlService = CottonDQLGrpc.newBlockingStub(channel)
        ddlService = CottonDDLGrpc.newBlockingStub(channel)
        dmlService = CottonDMLGrpc.newBlockingStub(channel)
    }

    /**
     * Initializes list of strings of autocomplete.
     */
    fun initCompletion() {
        val schemata = mutableListOf<Name.SchemaName>()
        val entities = mutableListOf<Name.EntityName>()
        this@CottontailCommand.ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
            schemata.add(_schema.fqn())
            this@CottontailCommand.ddlService.listEntities(_schema).forEach { _entity ->
                entities.add(_entity.fqn())
            }
        }
        Cli.updateArgumentCompletion(schemata = schemata.map { it.toString() }, entities = entities.map { it.toString() })
    }

    /**
     * Base class for none entity specific commands (for potential future generalisation)
     */
    abstract inner class AbstractCottontailCommand(name: String, help: String) : CliktCommand(name = name, help = help) {
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
    }

    /**
     * Base class for entity specific commands, providing / querying from user schema and entity as arguments
     */
    abstract inner class AbstractSchemaCommand(name: String, help: String) : AbstractCottontailCommand(name = name, help = help) {
        protected val schema: Name.SchemaName by argument(name = "schema", help = "The schema name this command targets at").convert { Name.SchemaName(*it.split(NAME_COMPONENT_DELIMITER).toTypedArray()) }
    }

    /**
     * Base class for entity specific commands, providing / querying from user schema and entity as arguments
     */
    abstract inner class AbstractEntityCommand(name: String, help: String) : AbstractCottontailCommand(name = name, help = help) {
        protected val entity: Name.EntityName by argument(name = "entity", help = "The entity name this command targets at").convert { Name.EntityName(*it.split(NAME_COMPONENT_DELIMITER).toTypedArray()) }
    }

    /**
     * Command to list available entities and schemata
     */
    inner class ListAllEntitiesCommand : AbstractCottontailCommand(name = "list", help = "Lists all entities in all schemata") {
        override fun exec() {
            this@CottontailCommand.ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
                this@CottontailCommand.ddlService.listEntities(_schema).forEach { _entity ->
                    println(_entity.fqn().toString())
                }
            }
        }
    }

    /**
     * Command to preview a given entity
     */
    inner class PreviewEntityCommand : AbstractEntityCommand(name = "preview", help = "Gives a preview of the entity specified") {
        private val limit: Long by option("-l", "--limit", help = "Limits the amount of printed results").long().default(10).validate { require(it > 0) }
        override fun exec() {
            println("Showing first $limit elements of entity $entity")
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(this.entity.protoFrom())
                            .setProjection(MatchAll())
                            .setLimit(this.limit)
            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            println("Previewing $limit elements of $entity:")
            query.forEach { result -> println(result.tuple) }
        }

    }

    /**
     * Command for entity details
     */
    inner class ShowEntityCommand : AbstractEntityCommand(name = "show", help = "Gives an overview of the entity and its columns") {
        override fun exec() {
            val details = this@CottontailCommand.ddlService.entityDetails(this.entity.proto())
            println("Entity ${details.entity.schema.name}.${details.entity.name} with ${details.columnsCount} columns: ")
            details.columnsList.forEach { println("\t${it.name}(type=${it.type}, size=${it.length}, unique=${it.unique}, nullable=${it.nullable})") }
        }
    }

    /**
     * Command to optimize an entity
     */
    inner class OptimizeEntityCommand : AbstractEntityCommand(name = "optimize", help = "Optimizes the specified entity, e.g. rebuilds the indices") {
        override fun exec() {
            println("Optimizing entity $entity. Please be patient...")
            this@CottontailCommand.ddlService.optimizeEntity(this.entity.proto())
            println("Optimization complete!")
        }
    }

    /**
     * Command to count elements in entity.
     */
    inner class CountEntityCommand : AbstractEntityCommand(name = "count", help = "Counts the given entity's rows") {
        override fun exec() {
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(this.entity.protoFrom())
                            .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.COUNT).build())

            ).build()
            val query = this@CottontailCommand.dqlService.query(qm)
            query.forEach { result -> println(result.tuple) }
        }
    }

    /**
     * Command to count elements in entity.
     */
    inner class QueryByColumnValueEqualsEntityCommand : AbstractEntityCommand(name = "find", help = "Find within an entity by column-value specification") {
        val col: String by option("-c", "--column", help = "Column name").required()
        val value: String by option("-v", "--value", help = "The value").required()
        override fun exec() {
            val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                    CottontailGrpc.Query.newBuilder()
                            .setFrom(this.entity.protoFrom())
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
            query.forEach { result ->
                if (result.hits > 0) {
                    println(result.tuple)
                    size += 1
                }
            }
            println("Found and printed $size results.")
        }
    }

    /**
     * Command to drop, i.e. remove the given entity
     */
    inner class DropEntityCommand : AbstractEntityCommand(name = "drop", help = "Drops the given entity from the database and deletes it therefore") {
        override fun exec() {
            this@CottontailCommand.ddlService.dropEntity(this.entity.proto())
            println("Successfully dropped entity ${this.entity}.")
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
            if (programName.contains(" ")) {
                addUsage(parameters, programName.split(" ")[1]) // hack to not include the base command
            } else {
                addUsage(parameters, programName)
            }
            addOptions(parameters)
            addArguments(parameters)
            addCommands(parameters)
        }
    }

    /**
     * Stops the entire application
     */
    inner class StopCommand : AbstractCottontailCommand(name = "stop", help = "Stops the database server and this CLI") {
        @ExperimentalTime
        override fun exec() {
            println("Stopping now...")
            this@CottontailCommand.channel.shutdown()
            Cli.stopServer()
        }
    }

    /**
     * Reconnects to Cottontail DB server.
     */
    inner class ReloadCommand : AbstractCottontailCommand(name = "reload", help = "Reload the connection to the DB server") {
        val host: String by option("-s", "--server", help = "The server address.").defaultLazy { this@CottontailCommand.host }
        val port: Int by option("-p", "--port", help = "The db port").int().defaultLazy { this@CottontailCommand.port }
        override fun exec() {
            println("Reconnecting...")
            this@CottontailCommand.initDbCon(host, port)
            println("Reconnected.")
        }
    }
}