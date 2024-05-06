package org.vitrivr.cottontail.dbms.index.deg

import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.dbms.index.diskann.graph.InMemoryDynamicExplorationGraph
import org.vitrivr.cottontail.test.TestConstants
import java.util.*

class InMemoryDEGTest {

    private val random = SplittableRandom()


    @Test
    public fun testWithFloatVector() {
        val size = random.nextInt(256)
        val type = Types.FloatVector(size)
        val distance = EuclideanDistance.FloatVector(type)
        val list = LinkedList<FloatVectorValue>()
        val graph = InMemoryDynamicExplorationGraph<TupleId, FloatVectorValue>(30) { v1, v2 -> distance.invoke(v1, v2).value.toFloat() }

        /* Build graph. */
        repeat(TestConstants.TEST_COLLECTION_SIZE) { it ->
            val next = FloatVectorValueGenerator.random(size, this.random)
            list.add(next)
            graph.index(it.toLong(), next)
        }

        println("Done")
    }
}