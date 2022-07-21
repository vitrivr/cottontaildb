package org.vitrivr.cottontail.dbms.index.pq

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.PQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.test.TestConstants
import java.util.*
import kotlin.math.log10
import kotlin.math.sqrt


/**
 * A series of tests for the data structures involved in the PQ-index implementation. These tests are executed for [FloatVectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class PQFloatVectorCodebookTest {
    /* The random number generator. */
    private val random = JDKRandomGenerator()

    /** The dimensionality of the test vector.  */
    private val dimensions = this.random.nextInt(128, 2048)

    /** The number of clusters to use. */
    private val numberOfClusters = this.random.nextInt(128, 256)

    /** The random data to test on. The data comes pre-clustered, so that meaningful tests can be performed. */
    private val testdata = List(TestConstants.TEST_COLLECTION_SIZE) { i ->
        FloatVectorValue(FloatArray(this.dimensions) {
            (i % numberOfClusters) + this.random.nextDouble(-1.0, 1.0).toFloat()
        })
    }

    /** The [EuclideanDistance] that is used for the tests. */
    private val distance = EuclideanDistance.FloatVector(Types.FloatVector(this.dimensions))

    /** The [PQIndexConfig] that is used for the tests. */
    private val config = PQIndexConfig(this.distance.signature.name, this.numberOfClusters, 8)

    /** The data to train the quantizer with. */
    private val trainingdata = this.testdata.filter { this.random.nextDouble() <= (100.0 * (log10(this.testdata.size.toDouble())) / this.testdata.size) }

    /** The [SingleStageQuantizer] that is used for the tests. */
    private val quantizer = SingleStageQuantizer.learnFromData(this.distance, this.trainingdata, this.config)

    /**
     * Tests serialization / deserialization of [PQSignature] objects.
     */
    @Test
    fun testSignatureSerialization() {
        for (t in this.testdata) {
            val signature = this.quantizer.quantize(t)
            val serialized = signature.toEntry()
            val signature2 = SPQSignature.fromEntry(serialized)
            Assertions.assertArrayEquals(signature.cells, signature2.cells)
        }
    }

    /**
     * Makes sure that the signature always points to the smallest distance in the lookup table created for the same vector
     */
    @Test
    fun testSignature() {
        val index = this.random.nextInt(0, this.testdata.size)
        val vector = this.testdata[index]
        val signature = this.quantizer.quantize(vector)
        val lat = this.quantizer.createLookupTable(vector)
        for ((i, l) in lat.data.withIndex()) {
            l.forEach { d -> Assertions.assertTrue(d >= l[signature.cells[i].toInt()]) }
        }
    }

    /**
     * Makes sure that the signature always points to the smallest distance in the lookup table created for the same vector
     */
    @Test
    fun testLookupTable() {
        val index = this.random.nextInt(0, this.testdata.size)
        val vector = this.testdata[index]
        val signature = this.quantizer.quantize(vector)
        val lat = this.quantizer.createLookupTable(vector)
        Assertions.assertEquals(sqrt(signature.cells.mapIndexed { i, c -> lat.data[i][c.toInt()] }.sum()), lat.approximateDistance(signature))
    }

    /**
     * Makes sure that the signature always points to the smallest distance in the lookup table created for the same vector
     */
    @Test
    fun testDistanceCalculation() {
        val index = this.random.nextInt(0, this.testdata.size)
        val query = this.testdata[index]
        val lat = this.quantizer.createLookupTable(query)
        val results = LinkedList<Triple<Int,Double,Double>>()
        for ((i, t) in this.testdata.withIndex()) {
            val signature = this.quantizer.quantize(t)
            val distance = this.distance(query, t)
            val approximation = lat.approximateDistance(signature)
            results.add(Triple(i, distance.value, approximation))
        }

        /* Check: The first  numberOfClusters entries should be assigned to the same cluster. */
        val sorted1 = results.sortedBy { it.second }
        val idx = sorted1[0].first % numberOfClusters
        for (i in 1 until Math.floorDiv(this.testdata.size, this.numberOfClusters)) {
            Assertions.assertEquals(idx,sorted1[i].first % numberOfClusters)
        }

        /* Check: The first numberOfClusters entries should be assigned to the same cluster. */
        val sorted2 = results.sortedBy { it.third }
        for (i in 1 until Math.floorDiv(this.testdata.size, this.numberOfClusters)) {
            Assertions.assertEquals(idx,sorted2[i].first % numberOfClusters)
        }
    }
}