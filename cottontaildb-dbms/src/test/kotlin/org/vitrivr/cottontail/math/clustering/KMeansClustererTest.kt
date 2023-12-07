package org.vitrivr.cottontail.math.clustering

import org.apache.commons.math3.ml.clustering.Clusterable
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.util.MathArrays
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.utilities.VectorUtility
import org.vitrivr.cottontail.utilities.math.clustering.KMeansClusterer

/**
 * A unit test for [KMeansClusterer].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class KMeansClustererTest {

    /** The [JDKRandomGenerator] used for this test*/
    private val random = JDKRandomGenerator()

    /** Dimensionality of the vectors to test. */
    private val d = this.random.nextInt(32, 512)

    /** The number of clusters to find. */
    private val k = this.random.nextInt(10, 25)

    /** The number of vectors to process. */
    private val vectors = this.random.nextInt(4000, 7500)

    @RepeatedTest(3)
    fun testWithDoubleVectors() {
        /** Determine seed. */
        val seed = System.currentTimeMillis().toInt()
        println("Trying to find ${this.k} clusters by k-means clustering for ${this.vectors} vectors (d = ${this.d}).")

        /** Prepare list of vectors. */
        val list = mutableListOf<DoubleVectorValue>()
        VectorUtility.randomDoubleVectorSequence(this.d, 5000, this.random).forEachRemaining {
            list.add(it)
        }

        /* Prepare clusterer. */
        val clusterer = KMeansClusterer(this.k, EuclideanDistance.DoubleVector(Types.DoubleVector(this.d)), JDKRandomGenerator(seed), 500)
        val reference = KMeansPlusPlusClusterer<Clusterable>(this.k, 500, { a, b -> MathArrays.distance(a, b) }, JDKRandomGenerator(seed))

        /* Perform clustering. */
        val clusters = clusterer.cluster(list)
        val referenceClusters = reference.cluster(list.map { object: Clusterable {
            private val point = it
            override fun getPoint(): DoubleArray = this.point.data
        } })


        for ((i, c) in clusters.withIndex()) {
            assertTrue(referenceClusters[i].center.point.contentEquals((c.center as DoubleVectorValue).data))
        }
    }
}