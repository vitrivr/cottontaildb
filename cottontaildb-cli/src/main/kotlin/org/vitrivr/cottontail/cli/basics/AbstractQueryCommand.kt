package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.jakewharton.picnic.Table
import io.grpc.StatusException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.csv.Csv
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToStream
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.serialization.valueSerializer
import org.vitrivr.cottontail.utilities.TabulationUtilities
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * An [AbstractCottontailCommand] that concerns the execution of queries.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
abstract class AbstractQueryCommand(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {

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
    ).convert { Format.valueOf(it) }.default(Format.CBOR)

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
    protected fun executeAndExport(query: org.vitrivr.cottontail.client.language.dql.Query) = executeAndExport(query.builder.build())

    /**
     * Takes a [Query], executes and collects all results into a specified output file using the specified format.
     *
     * @param query The [Query] to execute and export.
     */
    protected fun executeAndExport(query: CottontailGrpc.QueryMessage) {
        try {
            val duration = measureTime {
                val out = this.out
                if (out != null) {
                    /* Determine which data exporter to use. */
                    val format = this.format
                    if (format == null) {
                        println("Valid output format needs to be specified for export!")
                        return
                    }

                    /* Execute query. */
                    val results = if (this.explain) {
                        this.client.explain(query)
                    } else {
                        this.client.query(query)
                    }

                    /* Export data using kotlinx serialization */
                    Files.newOutputStream(this.out, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                        when (format) {
                            Format.CBOR -> it.write(Cbor.encodeToByteArray(ListSerializer(results.valueSerializer()), results.toList()))
                            Format.JSON -> Json.encodeToStream(ListSerializer(results.valueSerializer()), results.toList(), it)
                            Format.CSV -> it.write(Csv.encodeToString(ListSerializer(results.valueSerializer()), results.toList()).toByteArray())
                        }
                    }
                }
            }
            println("Executing and exporting query took $duration.")
        } catch (e: StatusException) {
            print("A ${e::class.java.simpleName} occurred while executing and exporting query: ${e.message}.")
        }
    }
}