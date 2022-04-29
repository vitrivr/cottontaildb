package org.vitrivr.cottontail.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.jakewharton.picnic.Table
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.TabulationUtilities
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Base class for none entity specific commands (for potential future generalisation)

 * @author Loris Sauter
 * @version 2.0.0
 */
sealed class AbstractCottontailCommand(name: String, help: String, val expand: Boolean) : CliktCommand(name = name, help = help) {

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

    /**
     * An [AbstractCottontailCommand] that concerns the inspection and management of the database system.
     */
    abstract class System(name: String, help: String): AbstractCottontailCommand(name, help, false)

    /**
     * An [AbstractCottontailCommand] that concerns the inspection and management of schemata.
     */
    abstract class Schema(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
        /** The [Name.SchemaName] affected by this [AbstractCottontailCommand.Schema]. */
        protected val schemaName: Name.SchemaName by argument(name = "schema", help = "The schema name targeted by the command. Has the form of [\"warren\"].<schema>.").convert {
            val split = it.split(Name.DELIMITER)
            when(split.size) {
                1 -> Name.SchemaName(split[0])
                2 -> {
                    require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                    Name.SchemaName(split[1])
                }
                else -> throw IllegalArgumentException("'$it' is not a valid schema name.")
            }
        }
    }

    /**
     * An [AbstractCottontailCommand] that concerns the inspection and management of entities.
     */
    abstract class Entity(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
        /** The [Name.EntityName] affected by this [AbstractCottontailCommand.Entity]. */
        protected val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
            val split = it.split(Name.DELIMITER)
            when(split.size) {
                1 -> throw IllegalArgumentException("'$it' is not a valid entity name. Entity name must contain schema specified.")
                2 -> Name.EntityName(split[0], split[1])
                3 -> {
                    require(split[0] == Name.ROOT) { "Invalid root qualifier ${split[0]}!" }
                    Name.EntityName(split[1], split[2])
                }
                else -> throw IllegalArgumentException("'$it' is not a valid entity name.")
            }
        }
    }

    /**
     * An [AbstractCottontailCommand] that can be used to perform queries.
     */
    @ExperimentalTime
    abstract class Query(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {

        /** Flag indicating, whether query should be executed or explained. */
        private val explain: Boolean by option(
            "-e",
            "--explain",
            help = "If set, query will only be executed and not set."
        ).convert { it.toBoolean() }.default(false)

        /** Flag indicating, whether query should be executed or explained. */
        private val out: Path? by option(
            "-o",
            "--out",
            help = "If set, query will be exported into files instead of CLI."
        ).convert { Paths.get(it) }

        /** Flag indicating, whether query should be executed or explained. */
        private val format: Format? by option(
            "-f",
            "--format",
            help = "Only for option --out; export format. Defaults to PROTO."
        ).convert { Format.valueOf(it) }.default(Format.PROTO)

        /** Flag indicating, whether query should written to file or console. */
        protected val toFile: Boolean
            get() = (this.out != null)

        /**
         * Takes a [Query], executes and collects all results into a [Table] and prints it to stdout.
         *
         * @param query The query to execute and print..
         */
        protected fun executeAndTabulate(query: org.vitrivr.cottontail.client.language.dql.Query)
            = executeAndTabulate(query.builder.build())

        /**
         * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [Table] and prints it to stdout.
         *
         * @param query The query to execute and print.
         */
        protected fun executeAndTabulate(query: CottontailGrpc.QueryMessage) {
            try {
                val results = measureTimedValue {
                    if (this.explain) {
                        TabulationUtilities.tabulate(this.client.explain(query))
                    } else {
                        TabulationUtilities.tabulate(this.client.query(query))
                    }
                }
                if (this.explain) {
                    println("Explaining query (took: ${results.duration}):")
                } else {
                    println("Executing query (took: ${results.duration}):")
                }
                print(results.value)
            } catch (e: StatusException) {
                print("An error occurred while executing query: ${e.message}.")
            }
        }

        /**
         * Takes a [Query], executes and collects all results into a specified output file using the specified format.
         *
         * @param query The [Query] to execute and export.
         */
        protected fun executeAndExport(query: org.vitrivr.cottontail.client.language.dql.Query)
            = executeAndExport(query.builder.build())

        /**
         * Takes a [Query], executes and collects all results into a specified output file using the specified format.
         *
         * @param query The [Query] to execute and export.
         */
        protected fun executeAndExport(query: CottontailGrpc.QueryMessage) {
            try {
                val duration = measureTime {
                    if (this.out != null) {
                        /* Determine which data exporter to use. */
                        val dataExporter = this.format?.newExporter(this.out!!)
                        if (dataExporter == null) {
                            println("Valid output format needs to be specified for export!")
                            return
                        }

                        /* Execute query. */
                        val results = if (this.explain) {
                            this.client.explain(query)
                        } else {
                            this.client.query(query)
                        }

                        /* Export data. */
                        for (r in results) {
                            dataExporter.offer(r)
                        }
                        dataExporter.close()
                    }
                }
                println("Executing and exporting query took $duration.")
            } catch (e: StatusException) {
                print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
            }
        }
    }
}