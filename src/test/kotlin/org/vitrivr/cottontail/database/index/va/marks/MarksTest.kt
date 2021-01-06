package org.vitrivr.cottontail.database.index.va.marks

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.testutils.getComplexVectorsFromFile
import java.util.*
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sign
import kotlin.reflect.full.createType
import kotlin.reflect.full.declaredMemberProperties

internal class MarksTest {
    val random = Random(1234)
    val numVecs = 100
    val numDim = 20
    val marksPerDim = 100
    val realdata = Array(numVecs) {
        DoubleArray(numDim) { random.nextGaussian() }
    }

    @Test
    fun getCells() {
        val marks = MarksGenerator.getEquidistantMarks(realdata, IntArray(numDim) { marksPerDim })
        val cells = marks.getCells(realdata.first())
        println("marks")
        for (m in marks.marks) {
            println(m.joinToString())
        }
        println("cells")
        println(cells.joinToString())
        realdata.forEach {
            marks.getCells(it).forEachIndexed { i, m ->
                assertTrue(it[i] >= marks.marks[i][m])
                assertTrue(it[i] <= marks.marks[i][m + 1])
            }
        }
    }
}