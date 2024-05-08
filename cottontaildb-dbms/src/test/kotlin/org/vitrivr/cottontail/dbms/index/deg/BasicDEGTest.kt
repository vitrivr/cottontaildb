package org.vitrivr.cottontail.dbms.index.deg

import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.dbms.index.diskann.graph.deg.AbstractDynamicExplorationGraph
import org.vitrivr.cottontail.dbms.index.diskann.graph.deg.InMemoryDynamicExplorationGraph
import org.vitrivr.cottontail.dbms.index.diskann.graph.deg.XodusDynamicExplorationGraph
import org.vitrivr.cottontail.dbms.index.diskann.graph.serializer.TupleIdNodeSerializer
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.formats.FVecsReader
import org.vitrivr.cottontail.utilities.math.ranking.RankingUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.*
import kotlin.time.Duration
import kotlin.time.measureTime


/**
 * This is a basic test case that makes sure that the [XodusDynamicExplorationGraph] and the [InMemoryDynamicExplorationGraph] work as expected.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BasicDEGTest {

    companion object {
        const val K = 100
    }

    /**
     * Tests the [XodusDynamicExplorationGraph] with 10'000 SIFT vectors ([FloatVectorValue]).
     */
    @Test
    fun testPersistentDEGWithSIFTVector() {
        Environments.newInstance(TestConstants.testConfig().root.toFile()).use { environment ->
            environment.executeInTransaction { txn ->
                /* Create a new store. */
                val store = environment.openStore("test", StoreConfig.WITH_DUPLICATES, txn, true)!!

                /* Prepare parameters. */
                val type = Types.FloatVector(128)
                val distance = EuclideanDistance.FloatVector(type)
                val list = LinkedList<FloatVectorValue>()
                val graph = XodusDynamicExplorationGraph<TupleId, FloatVectorValue>(4, 16, 0.2f, store, txn, TupleIdNodeSerializer()) { v1, v2 -> distance.invoke(v1, v2).value.toFloat() }

                /* Index vectors and build graph & ground truth. */
                this.index(graph)

                /* Perform search. */
                this.search(graph, distance)
            }
        }
    }

    /**
     * Tests the [InMemoryDynamicExplorationGraph] with 10'000 SIFT vectors ([FloatVectorValue]).
     */
    @Test
    fun testInMemoryDEGWithSIFTVector() {
        /* Prepare parameters. */
        val type = Types.FloatVector(128)
        val distance = EuclideanDistance.FloatVector(type)
        val graph = InMemoryDynamicExplorationGraph<TupleId, FloatVectorValue>(4, 16, 0.2f) { v1, v2 -> distance.invoke(v1, v2).value.toFloat() }

        /* Index vectors and build graph & ground truth. */
        this.index(graph)

        /* Perform search. */
        this.search(graph, distance)
    }

    /**
     * Indexes the SIFT test data.
     *
     * @param graph The [AbstractDynamicExplorationGraph] to add to.
     */
    private fun index(graph: AbstractDynamicExplorationGraph<TupleId, FloatVectorValue>) {
        /* Read vectors and build graph. */
        FVecsReader(this.javaClass.getResourceAsStream("/sift/siftsmall_base.fvecs")!!).use { reader ->
            var index = 1L
            while (reader.hasNext()) {
                val next = FloatVectorValue(reader.next())
                graph.index(index++, next)
            }
        }
    }

    /**
     * Searches the SIFT test data.
     *
     * @param graph The [AbstractDynamicExplorationGraph] to add to.
     * @param distance The [EuclideanDistance] function.
     */
    private fun search(graph: AbstractDynamicExplorationGraph<TupleId, FloatVectorValue>, distance: EuclideanDistance<FloatVectorValue>) {
        /* Fetch results through full table scan. */
        FVecsReader(this.javaClass.getResourceAsStream("/sift/siftsmall_query.fvecs")!!).use { reader ->
            var queries = 0
            var recall = 0.0f
            var bruteForceDuration = Duration.ZERO
            var indexDuration = Duration.ZERO

            while (reader.hasNext()) {
                val query = FloatVectorValue(reader.next())
                val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(K)
                bruteForceDuration += measureTime {
                    graph.vertices().use { vertices ->
                        var index = 1L
                        for (vertex in vertices) {
                            bruteForceResults.offer(ComparablePair(index++, distance.invoke(query, graph.getValue(vertex))!!))
                        }
                    }
                }

                /* Fetch results through index. */
                val indexResults = ArrayList<ComparablePair<TupleId, DoubleValue>>(K)
                indexDuration += measureTime {
                    graph.search(query, K, 0.2f).forEach { indexResults.add(ComparablePair(it.label, DoubleValue(it.distance))) }
                }
                recall += RankingUtilities.recallAtK(bruteForceResults.toList().map { it.first }, indexResults.map { it.first }, K)
                queries++
            }
            recall /= queries
            indexDuration /= queries
            bruteForceDuration /= queries

            /* Since the data comes pre-clustered, accuracy should always be greater than 90%. */
            Assertions.assertTrue(recall >= 0.8f) { "Recall attained by indexed search is too small (r = $recall)." }
            Assertions.assertTrue(bruteForceDuration >= indexDuration) { "Index search was slower than brute-force (withIndex = $indexDuration, bruteForce = $bruteForceDuration)." }
            println("Search using DEG completed (r = $recall, withIndex = $indexDuration, bruteForce = $bruteForceDuration). Brute-force duration is always in memory!")
        }
    }
}