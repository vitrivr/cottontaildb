package org.vitrivr.cottontail.dbms.index.va

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.values.generators.DoubleVectorValueGenerator
import org.vitrivr.cottontail.dbms.index.va.signature.VAFMarks
import java.lang.Double.max
import java.lang.Double.min

/**
 * Unit Test that can be used to test the [VAFMarks] implementation.
 *
 * @author Gabriel Zihlmann
 * @version 1.1.0
 */
class MarksTest {
    private val random = JDKRandomGenerator()
    private val numVecs = 1000
    private val numDim = 20
    private val marksPerDim = 100
    private val realdata = Array(this.numVecs) {
        DoubleVectorValueGenerator.random(this.numDim, this.random)
    }
    private val min = DoubleArray(this.numDim)
    private val max  = DoubleArray(this.numDim)

    init {
        for (d in this.realdata) {
            for (i in 0 until this.numDim) {
                this.min[i] = min(d.data[i], min[i])
                this.max[i] = max(d.data[i], max[i])
            }
        }

    }

    @Test
    fun getCells() {
        val marks: VAFMarks = VAFMarks.getEquidistantMarks(this.min, this.max, this.marksPerDim)
        this.realdata.forEach {
            marks.getSignature(it).cells.forEachIndexed { i, m ->
                assertTrue(it.data[i] >= marks.marks[i][m.toInt()])
                assertTrue(it.data[i] <= marks.marks[i][m.toInt() + 1])
            }
        }
    }
}