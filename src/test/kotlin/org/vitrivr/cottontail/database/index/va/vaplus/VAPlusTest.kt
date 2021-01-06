package org.vitrivr.cottontail.database.index.va.vaplus

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.database.index.va.marks.Marks
import org.vitrivr.cottontail.database.index.va.marks.MarksGenerator
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

internal class VAPlusTest {
    val vap = VAPlus()
    val random = Random(1234)
    val numVecs = 100
    val numDim = 20
    val marksPerDim = 100
    val realdata = Array(numVecs) {
        DoubleArray(numDim) { random.nextGaussian() }
    }
    val imaginarydata = Array(numVecs) { // imaginary parts
        DoubleArray(numDim) { random.nextGaussian() }
    }
    val realmarks = MarksGenerator.getEquidistantMarks(realdata, IntArray(realdata.first().size) { marksPerDim })

    @Test
    fun computeBounds() {
        val vector = realdata.first()
        val bounds = vap.computeBounds(vector, realmarks.marks)
        println("vector")
        println(vector.joinToString())
        println("bounds first (lbounds)")
        bounds.first.forEach { println(it.joinToString()) }
        println("bounds second (ubounds)")
        bounds.second.forEach { println(it.joinToString()) }
    }



}