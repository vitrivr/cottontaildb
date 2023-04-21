package org.vitrivr.cottontail.dbms.index.va

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.values.generators.DoubleVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.index.va.bounds.L1Bounds
import org.vitrivr.cottontail.dbms.index.va.bounds.L2Bounds
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.test.TestConstants
import java.lang.Double.max
import java.lang.Double.min

/**
 * Unit Test that can be used to test the [EquidistantVAFMarks] implementation.
 *
 * @author Gabriel Zihlmann
 * @version 1.1.0
 */
class EquidistantMarksTest {
    private val random = JDKRandomGenerator()

    /** The dimensionality of the test vector.  */
    private val dimensions = this.random.nextInt(128, 2048)

    /** The number of marks per dimension.  */
    private val marksPerDimension = this.random.nextInt(5, 30)

    /** The random data to test on. */
    private val testdata = Array(TestConstants.TEST_COLLECTION_SIZE) {
        DoubleVectorValueGenerator.random(dimensions, this.random)
    }

    /** The per-dimension minimum of the test data. */
    private val min = DoubleArray(dimensions) {d ->
        var min = Double.MAX_VALUE
        for (i in 0 until this.testdata.size) {
            min = min(this.testdata[i][d].value, min)
        }
        min
    }

    /** The per-dimension maximum of the test data. */
    private val max = DoubleArray(dimensions) { d ->
        var max = Double.MIN_VALUE
        for (i in 0 until this.testdata.size) {
            max = max(this.testdata[i][d].value, max)
        }
        max
    }

    /** The [EquidistantVAFMarks] to test. */
    private val marks = EquidistantVAFMarks(this.min, this.max, this.marksPerDimension)

    /**
     * Tests cell properties w.r.t to the testdata according to [1].
     */
    @RepeatedTest(3)
    fun testEquidistantMarks() {
        this.marks.minimum.forEachIndexed { i, d -> assertTrue(this.min[i] > d) }
        this.marks.maximum.forEachIndexed { i, d -> assertTrue(this.max[i] < d) }
    }

    /**
     * Tests cell properties w.r.t to the testdata according to [1].
     */
    @RepeatedTest(3)
    fun testGetCells() {
        /* Generate marks and check them. */
        this.testdata.forEach {
            val signature = this.marks.getSignature(it)
            signature.cells.forEachIndexed { i, m ->
                assertTrue(it.data[i] >= this.marks.marks[i][m.toInt()])
                assertTrue(it.data[i] <= this.marks.marks[i][m + 1])
            }
        }
    }

    @RepeatedTest(3)
    fun testL1Bounds() {
        val query = DoubleVectorValueGenerator.random(this.dimensions, this.random)
        val function = ManhattanDistance.DoubleVector(Types.DoubleVector(this.dimensions))
        val bounds = L1Bounds(query, this.marks)

        this.testdata.forEach {
            val signature =  this.marks.getSignature(it)
            val distance = function(query, it)
            val lb = bounds.lb(signature)
            val ub = bounds.ub(signature)
            assertTrue(lb <= distance.value)
            assertTrue(ub >= distance.value)
        }
    }


    @RepeatedTest(3)
    fun testL2Bounds() {
        val query = DoubleVectorValueGenerator.random(this.dimensions, this.random)
        val function = EuclideanDistance.DoubleVector(Types.DoubleVector(this.dimensions))
        val bounds = L2Bounds(query, this.marks)
        this.testdata.forEach {
            val signature =  this.marks.getSignature(it)
            val distance = function(query, it)
            val lb = bounds.lb(signature)
            val ub = bounds.ub(signature)
            assertTrue(lb <= distance.value)
            assertTrue(ub >= distance.value)
        }
    }

    @RepeatedTest(3)
    fun testSerialization() {
        /* Generate marks and check them. */
        this.testdata.forEach {
            val sig =  this.marks.getSignature(it)
            val serialized =  sig.toEntry()
            val sig2 = VAFSignature.fromEntry(serialized)
            assertArrayEquals(sig.cells, sig2.cells)
        }
    }
}