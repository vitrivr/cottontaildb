package org.vitrivr.cottontail.cli.benchmarks

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import org.vitrivr.cottontail.cli.basics.AbstractBenchmarkCommand
import org.vitrivr.cottontail.cli.benchmarks.model.Benchmark
import org.vitrivr.cottontail.cli.benchmarks.model.BenchmarkResult
import org.vitrivr.cottontail.cli.benchmarks.model.PRMeasure
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.ddl.CreateIndex
import org.vitrivr.cottontail.client.language.ddl.DropIndex
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType
import java.util.*

/**
 * A simple command that can be used to benchmark the performance of high-dimensional index structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HighDimensionalIndexBenchmark(client: SimpleClient): AbstractBenchmarkCommand(client, name = "hdindex", help = "Benchmarks the execution performance of NNS query using a HD-index vs. brute-force. Usage: benchmark hdindex [warren.]<schema>.<entity> <id-col> <feature-col> <index>") {

    /** The [Name.EntityName] of the entity benchmarked by this [HighDimensionalIndexBenchmark]. */
    private val entity: Name.EntityName by argument(name = "entity", help = "The fully qualified entity name of the entity that should be benchmarked. Has the form of [\"warren\"].<schema>.<entity>").convert {
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

    /** The column to benchmark. */
    private val idColumn by argument(name = "id", help = "The simple name of the ID column.")

    /** The column to benchmark. */
    private val featureColumn by argument(name = "id", help = "The simple name of the feature column.")

    /** The type of index to benchmark. */
    private val index by argument(name = "index", help = "The type of index to create.").enum<IndexType>()

    /** The number of repetitions to perform when executing the benchmark. */
    private val k: Long by option("-k", help = "Number of repetitions to perform while benchmarking.").convert { it.toLong() }.default(500L)


    override fun initialize(): Benchmark = object: Benchmark("HighDimensionalIndexBenchmark", 2){
        /** The query vector to use for this [Benchmark]. */
        private val baseline = LinkedList<Any>()

        /** The query vector to use for this [Benchmark]. */
        private var query: Query? = null

        /**
         * Prepares the baseline during phase 1 and creates the high-dimensional index before the start of phase 2
         *
         * @param phase The index of the phase to prepare.
         */
        override fun prepare(phase: Int) {
            when (phase) {
                1 -> {
                    /* Make sanity checks. */
                    val schema = client.readSchema(this@HighDimensionalIndexBenchmark.entity)
                    val featureColumn = schema.find { it.name == this@HighDimensionalIndexBenchmark.entity.column(this@HighDimensionalIndexBenchmark.featureColumn) }
                        ?: throw IllegalArgumentException("The feature column ${this@HighDimensionalIndexBenchmark.featureColumn} is not known for given entity.")

                    /* Obtain a distribution and a query vector. */
                    val vector = when (featureColumn.type) {
                        is Types.DoubleVector -> DoubleArray(featureColumn.type.logicalSize)
                        is Types.FloatVector ->  FloatArray(featureColumn.type.logicalSize)
                        else -> throw IllegalArgumentException("The selected column ${this@HighDimensionalIndexBenchmark.featureColumn} does not have a vector type.")
                    }

                    /* Create baseline result set. */
                    this.query = Query(this@HighDimensionalIndexBenchmark.entity.fqn)
                        .select(this@HighDimensionalIndexBenchmark.idColumn)
                        .distance(this@HighDimensionalIndexBenchmark.featureColumn, vector, Distances.L2, "distance")
                        .order("distance", Direction.ASC)
                        .limit(this@HighDimensionalIndexBenchmark.k)

                    this.query!!.withoutParallelism()

                    /* Generate baseline result-set + warmup */
                    for (t in this@HighDimensionalIndexBenchmark.client.query(this.query!!)) {
                        this.baseline.add(t[0]!!)
                    }
                    require(this.baseline.size == this@HighDimensionalIndexBenchmark.k.toInt()) { "Mismatch" }
                }
                2 -> {
                    val testIndex = "idx_test_${this@HighDimensionalIndexBenchmark.index.toString().lowercase()}"
                    this@HighDimensionalIndexBenchmark.client.create(CreateIndex(
                        this@HighDimensionalIndexBenchmark.entity.fqn,
                        this@HighDimensionalIndexBenchmark.featureColumn,
                        this@HighDimensionalIndexBenchmark.index
                    ).name(testIndex).rebuild())
                }
                else -> throw IllegalStateException("Phase index $phase is out of bounds for this benchmark instance.")
            }
        }

        /**
         * Drops the high-dimensional index after the end of phase 2
         *
         * @param phase The index of the phase to clean-up.
         */
        override fun cleanup(phase: Int) {
            if (phase == 2) {
                val testIndex = "idx_test_${this@HighDimensionalIndexBenchmark.index.toString().lowercase()}"
                this@HighDimensionalIndexBenchmark.client.drop(DropIndex(this@HighDimensionalIndexBenchmark.entity.index(testIndex).fqn))
            }
        }

        /**
         * Performs a single warmup query per phase.
         *
         * @param phase The phase index to perform the warmup  for.
         * @param repetition The repetition.
         */
        override fun warmup(phase: Int, repetition: Int) {
            for (t in this@HighDimensionalIndexBenchmark.client.query(this.query!!)) {
                /* No op. */
            }
        }

        /**
         * Executes the actual workload for this [HighDimensionalIndexBenchmark].
         */
        override fun workload(phase: Int, repetition: Int): BenchmarkResult {
            val start = System.currentTimeMillis()
            val result = this@HighDimensionalIndexBenchmark.client.query(this.query!!)
            val end = System.currentTimeMillis()
            val prgraph = PRMeasure.generate(this.baseline, result)
            return if (phase == 1) {
                BenchmarkResult("Brute-force", "${this@HighDimensionalIndexBenchmark.entity}", phase, repetition, start, end, end-start, prgraph)
            } else {
                BenchmarkResult("Index", "${this@HighDimensionalIndexBenchmark.entity},${this@HighDimensionalIndexBenchmark.index}", phase, repetition, start, end, end-start, prgraph)
            }
        }

    }
}