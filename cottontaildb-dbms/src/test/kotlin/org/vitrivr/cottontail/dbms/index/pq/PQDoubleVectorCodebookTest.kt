package org.vitrivr.cottontail.dbms.index.pq

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableSingleStageProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.PQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.test.TestConstants
import java.lang.Math.floorDiv
import java.util.*
import kotlin.math.sqrt

/**
 * A series of tests for the data structures involved in the PQ-index implementation. These tests are executed for [DoubleVectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class PQDoubleVectorCodebookTest {
    /* The random number generator. */
    private val random = JDKRandomGenerator()

    /** The dimensionality of the test vector.  */
    private val dimensions = this.random.nextInt(128, 2048)

    /** The number of clusters to use. */
    private val numberOfClusters = 256

    /** The random data to test on. The data comes pre-clustered, so that meaningful tests can be performed. */
    private val testdata = List(TestConstants.TEST_COLLECTION_SIZE) {i ->
        DoubleVectorValue(DoubleArray(this.dimensions) {
            (i % numberOfClusters) + this.random.nextDouble(-1.0, 1.0)
        })
    }

    /** The [EuclideanDistance] that is used for the tests. */
    private val distance = EuclideanDistance.DoubleVector(Types.DoubleVector(this.dimensions))

    /** The [PQIndexConfig] that is used for the tests. */
    private val config = PQIndexConfig(this.distance.signature.name, 256, 8)

    /** The data to train the quantizer with. */
    private val trainingdata = this.testdata.filter { this.random.nextDouble() <= ((10.0 * this.numberOfClusters) / this.testdata.size) }

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
     * Tests serialization / deserialization of [SingleStageQuantizer] and the associated codebooks.
     */
    @Test
    fun testCodebookSerialization() {
        val serializable1 = this.quantizer.toSerializableProductQuantizer()
        val serialized = SerializableSingleStageProductQuantizer.Binding.objectToEntry(serializable1)
        val serializable2 = SerializableSingleStageProductQuantizer.Binding.entryToObject(serialized) as SerializableSingleStageProductQuantizer

        /* Compare SerializableProductQuantizer. */
        Assertions.assertEquals(serializable1, serializable2)

        /* Compare reconstructed quantizer. */
        val original = serializable2.toProductQuantizer(this.distance)
        Assertions.assertEquals(this.quantizer, original)
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
        val index1 = this.random.nextInt(0, this.testdata.size)
        val index2 = this.random.nextInt(0, this.testdata.size)

        /* Obtain to vectors. */
        val vector1 = this.testdata[index1]
        val vector2 = this.testdata[index2]

        /* Test definition of lookup table. */
        val signature = this.quantizer.quantize(vector2)
        val lat = this.quantizer.createLookupTable(vector1)
        Assertions.assertEquals(sqrt(signature.cells.mapIndexed { i, c -> lat.data[i][c.toInt()] }.sum()), lat.approximateDistance(signature))
    }

    /**
     * Makes sure that the signature always points to the smallest distance in the lookup table created for the same vector
     */
    @RepeatedTest(3)
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

        /* Check: The first numberOfClusters entries should be assigned to the same cluster. */
        val sorted1 = results.sortedBy { it.second }
        val idx = sorted1[0].first % numberOfClusters
        for (i in 0 until floorDiv(this.testdata.size, this.numberOfClusters)) {
            Assertions.assertEquals(idx,sorted1[i].first % numberOfClusters)
        }

        /* Check: The first numberOfClusters entries should be assigned to the same cluster. */
        val sorted2 = results.sortedBy { it.third }
        for (i in 0 until floorDiv(this.testdata.size, this.numberOfClusters)) {
            Assertions.assertEquals(idx,sorted2[i].first % numberOfClusters)
        }
    }
}