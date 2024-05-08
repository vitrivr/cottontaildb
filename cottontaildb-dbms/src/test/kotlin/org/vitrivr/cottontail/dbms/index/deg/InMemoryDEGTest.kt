package org.vitrivr.cottontail.dbms.index.deg

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.dbms.index.diskann.graph.InMemoryDynamicExplorationGraph
import org.vitrivr.cottontail.utilities.formats.FVecsReader
import org.vitrivr.cottontail.utilities.math.ranking.RankingUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.*
import kotlin.time.Duration
import kotlin.time.measureTime

class InMemoryDEGTest {

    private val random = SplittableRandom()

    @Test
    public fun testWithSIFTFloatVector() {
        val k = 100
        val type = Types.FloatVector(128)
        val distance = EuclideanDistance.FloatVector(type)
        val list = LinkedList<FloatVectorValue>()
        val graph = InMemoryDynamicExplorationGraph<TupleId, FloatVectorValue>(4) { v1, v2 -> distance.invoke(v1, v2).value.toFloat() }

        /* Read vectors and build graph. */
        FVecsReader(this.javaClass.getResourceAsStream("/sift/siftsmall_base.fvecs")!!).use { reader ->
            while (reader.hasNext()) {
                val next = FloatVectorValue(reader.next())
                list.add(next)
                graph.index(list.size.toLong(), next)
            }
        }

        /* Fetch results through full table scan. */
        FVecsReader(this.javaClass.getResourceAsStream("/sift/siftsmall_query.fvecs")!!).use { reader ->
            var queries = 0
            var recall = 0.0f
            var bruteForceDuration = Duration.ZERO
            var indexDuration = Duration.ZERO

            while (reader.hasNext()) {
                val query = FloatVectorValue(reader.next())
                val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(100)
                bruteForceDuration += measureTime {
                    list.forEachIndexed { index, vector ->
                        bruteForceResults.offer(ComparablePair((index + 1L), distance(query, vector)))
                    }
                }

                /* Fetch results through index. */
                val indexResults = ArrayList<ComparablePair<TupleId, DoubleValue>>(k)
                indexDuration += measureTime {
                    graph.search(query, k, 0.2f).forEach { indexResults.add(ComparablePair(it.identifier.identifier, DoubleValue(it.distance))) }
                }
                recall += RankingUtilities.recallAtK(bruteForceResults.toList().map { it.first }, indexResults.map { it.first }, k)
                queries++
            }
            recall /= queries
            indexDuration /= queries
            bruteForceDuration /= queries

            /* Since the data comes pre-clustered, accuracy should always be greater than 90%. */
            Assertions.assertTrue(recall >= 0.8f) { "Recall attained by indexed search is too small (r = $recall)." }
            Assertions.assertTrue(bruteForceDuration > indexDuration) { "Brute-force search was faster ($bruteForceDuration) than indexed search ($indexDuration)." }
        }
    }
}