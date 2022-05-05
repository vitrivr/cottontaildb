package org.vitrivr.cottontail.cli.benchmarks

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.stat.correlation.StorelessCovariance
import org.apache.commons.math3.stat.descriptive.moment.VectorialMean
import org.vitrivr.cottontail.cli.basics.AbstractBenchmarkCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.ddl.CreateIndex
import org.vitrivr.cottontail.client.language.ddl.DropIndex
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.ColumnDef
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
    private val k: Long by option("-k", help = "Number of repetitions to perform while benchmarking.").convert { it.toLong() }.default(5000L)

    /** The query vector to use for this [HighDimensionalIndexBenchmark]. */
    private var query: Query? = null

    /** The query vector to use for this [HighDimensionalIndexBenchmark]. */
    private val baseline = LinkedList<Any>()

    /** The name of this [HighDimensionalIndexBenchmark]. */
    override val name: String = "HighDimensionalIndexBenchmark"

    /** The number of phases for this [HighDimensionalIndexBenchmark] is always two. */
    override val phases: Int = 2

    /**
     * Prepares the baseline during phase 1 and creates the high-dimensional index before the start of phase 2
     *
     * @param phase The index of the phase to prepare.
     */
    override fun prepare(phase: Int) {
        when (phase) {
            1 -> {
                /* Make sanity checks. */
                val schema = client.readSchema(this.entity)
                val featureColumn = schema.find { it.name == this.entity.column(this.featureColumn) } ?: throw IllegalArgumentException("The feature column ${this.featureColumn} is not known for given entity.")

                /* Obtain a distribution and a query vector. */
                val vector = when (featureColumn.type) {
                    is Types.DoubleVector -> DoubleArray(featureColumn.type.logicalSize)
                    is Types.FloatVector ->  FloatArray(featureColumn.type.logicalSize)
                    else -> throw IllegalArgumentException("The selected column ${this.featureColumn} does not have a vector type.")
                }

                /* Create baseline result set. */
                this.query = Query(this.entity.fqn)
                    .select(this.idColumn)
                    .distance(this.featureColumn, vector, Distances.L1, "distance")
                    .order("distance", Direction.ASC)
                    .limit(this@HighDimensionalIndexBenchmark.k)

                this.query!!.withoutParallelism()

                /* Generate baseline result-set + warmup */
                for (t in this.client.query(this.query!!)) {
                    this.baseline.add(t[0]!!)
                }
            }
            2 -> {
                val testIndex = "idx_test_${this.index.toString().lowercase()}"
                this.client.create(CreateIndex(this.entity.fqn, this.featureColumn, this.index).name(testIndex).rebuild())
            }
            else -> throw IllegalStateException("Phase index $phase is out of bounds for this benchmark instance.")
        }
    }

    /**
     * Drops the high-dimensional index after the end of phase 2
     *
     * @param phase The index of the phase to cleanup.
     */
    override fun cleanup(phase: Int) {
        if (phase == 2) {
            val testIndex = "idx_test_${this.index.toString().lowercase()}"
            this.client.drop(DropIndex(this.entity.index(testIndex).fqn))
        }
    }

    override fun warmup(phase: Int, repetition: Int) {
       /* */
    }

    /**
     * Executes the actual workload for this [HighDimensionalIndexBenchmark].
     */
    override fun workload(phase: Int, repetition: Int): BenchmarkResult {
        val start = System.currentTimeMillis()
        val result = this@HighDimensionalIndexBenchmark.client.query(this.query!!)
        val end = System.currentTimeMillis()
        val (rankAccuracy, overlapAccuracy) = ResultsetComparison.compare(baseline, result)
        return BenchmarkResult(this.name, "${this.entity},${this.index}", phase, repetition, start, end, end-start, rankAccuracy, overlapAccuracy)
    }

    /**
     * Obtains a [MultivariateNormalDistribution] for the given
     */
    private fun obtainDistribution(column: ColumnDef<*>): MultivariateNormalDistribution {
        val covariance = StorelessCovariance(column.type.logicalSize)
        val mean = VectorialMean(column.type.logicalSize)
        val results = this.client.query(Query().sample(column.name.entity()!!.fqn, 0.05f).limit(1000))
        val vector = DoubleArray(column.type.logicalSize)
        for (t in results) {
            when (column.type) {
                is Types.DoubleVector -> t.asDoubleVector(1)!!.forEachIndexed { i, d -> vector[i] = d }
                is Types.FloatVector -> t.asFloatVector(1)!!.forEachIndexed { i, d -> vector[i] = d.toDouble() }
                else -> throw IllegalArgumentException("The selected column ${this.featureColumn} does not have a vector type.")
            }
            covariance.increment(vector)
            mean.increment(vector)
        }
        return MultivariateNormalDistribution(mean.result, covariance.data)
    }
}