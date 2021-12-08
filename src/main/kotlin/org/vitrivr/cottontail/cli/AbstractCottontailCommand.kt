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
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.data.Format
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
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
     *
     */
    abstract class System(name: String, help: String): AbstractCottontailCommand(name, help, false)

    /**
     *
     */
    abstract class Schema(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
        /** The [Name.SchemaName] affected by this [AbstractCottontailCommand.Schema]. */
        protected val schemaName: Name.SchemaName by argument(name = "schema", help = "The schema name targeted by the command. Has the form of [\"warren\"].<schema>.").convert {
            Name.SchemaName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
        }
    }

    /*
     *
     */
    abstract class Entity(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {
        /** The [Name.EntityName] affected by this [AbstractCottontailCommand.Entity]. */
        protected val entityName: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name targeted by the command. Has the form of [\"warren\"].<schema>.<entity>").convert {
            Name.EntityName(*it.split(Name.NAME_COMPONENT_DELIMITER).toTypedArray())
        }
    }

    /**
     *
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
            help = "Only for option --out; export format. Defaults to PROTO"
        ).convert { Format.valueOf(it) }

        /** Flag indicating, whether query should written to file or console. */
        protected val toFile: Boolean
            get() = (this.out != null)

        /**
         * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [Table] and
         * prints it to stdout.
         *
         * @param query The query to execute and print..
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
         * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a specified
         * output file using the specified format.
         *
         * @param query The query to execute and export.
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