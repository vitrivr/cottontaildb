package org.vitrivr.cottontail.cli.benchmarks

import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.enum
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.cli.basics.AbstractBenchmarkCommand
import org.vitrivr.cottontail.cli.benchmarks.model.Benchmark
import org.vitrivr.cottontail.cli.benchmarks.model.BenchmarkResult
import org.vitrivr.cottontail.cli.benchmarks.model.PRMeasure
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.ddl.CreateIndex
import org.vitrivr.cottontail.client.language.ddl.DropIndex
import org.vitrivr.cottontail.client.language.ddl.TruncateEntity
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.data.importer.DataImporter
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.ArrayBlockingQueue

/**
 * A command that can be used to benchmark index performance over time
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AdaptiveIndexBenchmark(client: SimpleClient): AbstractBenchmarkCommand(client, name = "adm", help = "Benchmarks the execution performance of NNS query using a HD-index vs. brute-force. Usage: benchmark hdindex [warren.]<schema>.<entity> <id-col> <feature-col> <index>") {

    /** The [Name.EntityName] of the entity benchmarked by this [AdaptiveIndexBenchmark]. */
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
    private val k: Long by option("-k", help = "The number of entries to retrieve during NNS.").convert { it.toLong() }.default(500L)

    /** The [Path] to the input file. */
    private val input: Path by option("-i", "--input", help = "The path to the file that contains the data to import.").convert { Paths.get(it) }.required()

    /** The [Path] to the input file. */
    private val duration: Int by option("-d", "--duration", help = "The total duration in seconds this benchmark should run for.").convert { it.toInt() }.required()

    /** The duration over which the benchmark should be run. */
    private val initSize: Int by option("--init-size", help = "The number of entries to insert before starting the benchmark.").convert { it.toInt() }.required()

    /** The minimum number of inserts per cycle.*/
    val minInsertSize: Int by option("--min-insert", help = "The minimum number of tuples to insert per cycle.").convert { it.toInt() }.default(50)

    /** The maximum number of inserts per cycle. */
    val maxInsertSize: Int by option("--max-insert", help = "The maximum number of tuples to insert per cycle.").convert { it.toInt() }.default(200)

    /** The maximum number of inserts per cycle. */
    val minInsertTimeout: Long by option("--min-insert-timeout", help = "The minimum timeout in milliseconds between inserts.").convert { it.toLong() }.default(0L)

    /** The maximum number of inserts per cycle. */
    val maxInsertTimeout: Long by option("--max-insert-timeout", help = "The maximum timeout in milliseconds between insert.").convert { it.toLong() }.default(1000L)

    /* This type of benchmark does not know repetitions. */
    override val repeat: Int = 0

    /* This type of benchmark does not know warmups. */
    override val warmup: Int = 0

    /**
     * Executes the benchmark.
     */
    override fun exec() {
        /** Initialize the benchmark. */
        try {
            val benchmark = this.initialize()

            /** Prepare the benchmark. */
            println("Starting ${benchmark.name} phase 1 out of ${benchmark.phases}.")
            println("${benchmark.name} (1/${benchmark.phases}): Preparing benchmark...")
            benchmark.prepare(1)

            /* Start the actual benchmark and run it, until end of duration is reached. */
            try {
                val start = System.currentTimeMillis()
                val deadline = this.duration * 1000L
                var repetition = 1
                while (System.currentTimeMillis() - start < deadline) {
                    print("${benchmark.name} (1/${benchmark.phases}): Executing benchmark workload (${repetition++})...")
                    val result = benchmark.workload(1, repetition)
                    println(", took: ${result.durationMs}ms")
                    this.out?.write(result.toCSVLine())
                    this.out?.newLine()
                    if (repetition % 10 == 0) {
                        this.out?.flush()
                    }

                    /* Yield to other threads, if needed. */
                    Thread.yield()
                }
            } catch (e: Throwable) {
                System.err.println("Benchmark phase 1 failed: ${e.message}")
                return
            } finally {
                System.err.println("Benchmark phase 1 completed!")
                benchmark.cleanup(1)
            }
        } finally {
            this.out?.flush()
            this.out?.close()
        }
    }

    /**
     * Initializes the [Benchmark] associated with this [AdaptiveIndexBenchmark] class.
     */
    override fun initialize(): Benchmark = object: Benchmark("HighDimensionalIndexBenchmark", 1) {

        /** The schema of the selected  */
        private val schema = this@AdaptiveIndexBenchmark.client.readSchema(this@AdaptiveIndexBenchmark.entity)

        /** The name of the feature column.  */
        private val featureColumn = schema.find { it.name == this@AdaptiveIndexBenchmark.entity.column(this@AdaptiveIndexBenchmark.featureColumn) }
            ?: throw IllegalArgumentException("The feature column ${this@AdaptiveIndexBenchmark.featureColumn} is not known for given entity.")

        /** The [DataImporter] used to import data. */
        private val importer: DataImporter = Format.detectFormatForPath(this@AdaptiveIndexBenchmark.input).newImporter(this@AdaptiveIndexBenchmark.input, schema)

        /** The [SplittableRandom] instance used to generate random values. */
        private val random = SplittableRandom()

        /** An [ArrayBlockingQueue] holding the next query vectors. */
        private var queryVector: Any? = null

        /** The name of the test index. */
        private val testIndex = "idx_test_${this@AdaptiveIndexBenchmark.index.toString().lowercase()}"


        /** Flag indicating, that this benchmark is running. */
        @Volatile
        private var running: Boolean = true

        /** A background thread that continuously inserts data while this benchmark is running. */
        private var inserterThread: Thread = Thread {
            /* Now while benchmark is running, insert tuples. */
            while (this.running) {
                /* Start insert transaction with a random amount of inserts*/
                val inserts = this.random.nextInt(this@AdaptiveIndexBenchmark.minInsertSize, this@AdaptiveIndexBenchmark.maxInsertSize)
                val txId = this@AdaptiveIndexBenchmark.client.begin()
                val batchedInsert = BatchInsert(this@AdaptiveIndexBenchmark.entity.fqn)
                    .columns(*schema.map { it.name.simple }.toTypedArray())
                    .txId(txId)

                var inserted = 0
                while (this.importer.hasNext() && inserted++ < inserts) {
                    val next = this.importer.next()
                    batchedInsert.append(*schema.map { next[it] }.toTypedArray())
                }

                /* Perform insert and commit changes. */
                this@AdaptiveIndexBenchmark.client.insert(batchedInsert)
                this@AdaptiveIndexBenchmark.client.commit(txId)

                /* Sleep for some random amount */
                Thread.sleep(this.random.nextLong(this@AdaptiveIndexBenchmark.minInsertTimeout, this@AdaptiveIndexBenchmark.maxInsertTimeout))
            }
        }

        /**
         * Makes the necessary preparations for this [AdaptiveIndexBenchmark].
         *
         * @param phase The phase index, which is always 1.
         */
        override fun prepare(phase: Int) {
            /* Truncate entity and drop index if it exists. */
            this@AdaptiveIndexBenchmark.client.truncate(TruncateEntity(this@AdaptiveIndexBenchmark.entity.fqn))
            try {
                this@AdaptiveIndexBenchmark.client.drop(DropIndex(this@AdaptiveIndexBenchmark.entity.index(this.testIndex).fqn))
            } catch (e: StatusRuntimeException) {
                if (e.status.code != Status.Code.NOT_FOUND) {
                    throw e
                }
            }

            /* Insert all the data. */
            val txId = this@AdaptiveIndexBenchmark.client.begin()
            var inserted = 0
            try {
                /* Perform insert before the benchmarks. */
                val batchedInsert = BatchInsert(this@AdaptiveIndexBenchmark.entity.fqn).columns(*schema.map { it.name.simple }.toTypedArray()).txId(txId)
                while (inserted < this@AdaptiveIndexBenchmark.initSize && this.importer.hasNext()) {
                    inserted += 1
                    val next = this.importer.next()
                    batchedInsert.append(*this.schema.map { next[it] }.toTypedArray())
                    if (this.random.nextBoolean()) {
                        this.queryVector = next[this.featureColumn]
                    }
                    if ((batchedInsert.builder.build().serializedSize) >= Constants.MAX_PAGE_SIZE_BYTES) {
                        this@AdaptiveIndexBenchmark.client.insert(batchedInsert)
                        batchedInsert.builder.clearInserts()
                    }
                }

                /** Insert remainder. */
                if (batchedInsert.builder.insertsCount > 0) {
                    this@AdaptiveIndexBenchmark.client.insert(batchedInsert)
                }

                /** Commit transaction, if single transaction option has been set. */
                this@AdaptiveIndexBenchmark.client.commit(txId)
            } catch (e: Throwable) {
                this@AdaptiveIndexBenchmark.client.rollback(txId)
                throw e
            }

            /* Build the index. */
            this@AdaptiveIndexBenchmark.client.create(CreateIndex(this@AdaptiveIndexBenchmark.entity.fqn, this@AdaptiveIndexBenchmark.featureColumn, this@AdaptiveIndexBenchmark.index).name(testIndex).rebuild())

            /* Start the importer thread. */
            this.inserterThread.start()
        }

        /**
         * No warmup queries are being performed.
         */
        override fun warmup(phase: Int, repetition: Int) {
            /* No op. */
        }

        /**
         * Executes the actual workload for this [HighDimensionalIndexBenchmark].
         *
         * By design, it does not exhibit any repetitions.
         *
         * @param phase The phase index.
         * @param repetition The repetition index.
         */
        override fun workload(phase: Int, repetition: Int): BenchmarkResult {
            val txId = this@AdaptiveIndexBenchmark.client.begin()
            val count = this@AdaptiveIndexBenchmark.client.query(Query(this@AdaptiveIndexBenchmark.entity.fqn).count().txId(txId)).next().asLong(0)!!
            val q1 = Query(this@AdaptiveIndexBenchmark.entity.fqn)
                .select(this@AdaptiveIndexBenchmark.idColumn)
                .distance(this@AdaptiveIndexBenchmark.featureColumn, this.queryVector!!, Distances.L2, "distance")
                .order("distance", Direction.ASC)
                .limit(this@AdaptiveIndexBenchmark.k)
                .disallowIndex()
                .txId(txId)

            /* Collect baseline. */
            val baseline = LinkedList<Any>()
            for (t in this@AdaptiveIndexBenchmark.client.query(q1)) {
                baseline.add(t[this@AdaptiveIndexBenchmark.idColumn]!!)
            }

            /* Execute same query, but with index and without parallelism. */
            val q2 = Query(this@AdaptiveIndexBenchmark.entity.fqn)
                .select(this@AdaptiveIndexBenchmark.idColumn)
                .distance(this@AdaptiveIndexBenchmark.featureColumn, this.queryVector!!, Distances.L2, "distance")
                .order("distance", Direction.ASC)
                .limit(this@AdaptiveIndexBenchmark.k)
                .disallowParallelism()
                .txId(txId)

            val start = System.currentTimeMillis()
            val test = this@AdaptiveIndexBenchmark.client.query(q2)
            val end = System.currentTimeMillis()
            val prgraph = PRMeasure.generate(baseline, test)

            /* Rollback transaction and return results. */
            this@AdaptiveIndexBenchmark.client.rollback(txId)
            return BenchmarkResult("Adaptive Index Management", "${this@AdaptiveIndexBenchmark.entity},${this@AdaptiveIndexBenchmark.index}", phase, repetition, start, end, end-start, count, prgraph)
        }

        /**
         * Performs the cleanup after the benchmark has concluded.
         */
        override fun cleanup(phase: Int) {
            this.running = false

            /* Close the importer. */
            this.importer.close()

            /* Drop the index. */
            val testIndex = "idx_test_${this@AdaptiveIndexBenchmark.index.toString().lowercase()}"
            this@AdaptiveIndexBenchmark.client.drop(DropIndex(this@AdaptiveIndexBenchmark.entity.index(testIndex).fqn))
        }
    }
}