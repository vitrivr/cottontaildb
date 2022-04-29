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
import org.vitrivr.cottontail.grpc.CottontailGrpc
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue


/**
 *
 */
class HDIndexBenchmark(client: SimpleClient): AbstractBenchmarkCommand(client, name = "adm", help = "Clears the given entity, i.e., deletes all data it contains. Usage: entity clear <schema>.<entity>") {

    /** The [Name.EntityName] of the entity benchmarked by this [HDIndexBenchmark]. */
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
    private val index by argument(name = "index", help = "The type of index to create.").enum<CottontailGrpc.IndexType>()

    /** The number of repetitions to perform when executing the benchmark. */
    private val k: Long by option("-k", help = "Number of repetitions to perform while benchmarking.").convert { it.toLong() }.default(100L)

    @OptIn(ExperimentalTime::class)
    override fun exec() {
        /* Make sanity checks. */
        val schema = client.readSchema(this.entity)
        val idColumn = schema.find { it.name == this.entity.column(this.idColumn) } ?: throw IllegalArgumentException("The ID column ${this.idColumn} is not known for given entity.")
        val featureColumn = schema.find { it.name == this.entity.column(this.featureColumn) } ?: throw IllegalArgumentException("The feature column ${this.featureColumn} is not known for given entity.")

        /* Obtain a distribution and a query vector. */
        val distribution = this.obtainDistribution(featureColumn)
        val queryVector = when (featureColumn.type) {
            is Types.DoubleVector -> distribution.sample()
            is Types.FloatVector ->  distribution.sample().map { it.toFloat() }.toFloatArray()
            else -> throw IllegalArgumentException("The selected column ${this.featureColumn} does not have a vector type.")
        }

        /* Create baseline result set. */
        val query = Query(this.entity.fqn)
            .select(this.idColumn)
            .distance(this.featureColumn, queryVector, Distances.L2, "distance")
            .order("distance", Direction.ASC)
            .limit(this@HDIndexBenchmark.k)

        query.withoutParallelism()

        val baseline = mutableListOf<Any>()
        for (t in this.client.query(query)) {
            baseline.add(t[0]!!)
        }

        /* Create index. */
        val testIndex = "idx_test_${this.index.toString().lowercase()}"
        this.client.create(CreateIndex(this.entity.fqn, this.featureColumn, this.index).name(testIndex).rebuild())

        try {
            /* Run the benchmark. */
            repeat(this@HDIndexBenchmark.repeat) { repetition ->
                val durationAndResult = measureTimedValue { this@HDIndexBenchmark.client.query(query) }
                var hits = 0.0
                var i = 0
                for (t in durationAndResult.value) {
                    if (baseline[i++] == t[0]) hits += 1.0
                }
                this@HDIndexBenchmark.out(repetition, System.currentTimeMillis(), durationAndResult.duration.toDouble(DurationUnit.MILLISECONDS), hits/baseline.size)
            }
        } finally {
            /* Drop index again. */
            this.client.drop(DropIndex(this.entity.index(testIndex).fqn))
        }
    }

    /**
     * Obtains a [MultivariateNormalDistribution] for the given
     */
    private fun obtainDistribution(column: ColumnDef<*>): MultivariateNormalDistribution {
        val covariance = StorelessCovariance(column.type.logicalSize)
        val mean = VectorialMean(column.type.logicalSize)
        val results = this.client.query(Query().sample(column.name.entity()!!.fqn, 0.3f).limit(1000))
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

    /**
     *
     */
    private fun out(i: Int, timestamp: Long,  durationMs: Double, accuracy: Double) {
        println("{'index': $i, 'timestamp': ${timestamp}, 'duration': $durationMs, 'accuracy': $accuracy'}")
    }
}