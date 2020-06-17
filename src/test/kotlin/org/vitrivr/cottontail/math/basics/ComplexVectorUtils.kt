package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.complex.Complex
import org.apache.commons.math3.complex.ComplexField
import org.apache.commons.math3.linear.ArrayFieldVector
import org.apache.commons.math3.linear.ArrayRealVector
import org.apache.commons.math3.linear.FieldVector
import org.apache.commons.math3.linear.FieldVectorPreservingVisitor
import org.junit.jupiter.api.Assertions
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

fun isApproximatelyTheSame(expected: Float, actual: Float) {
    val DELTA = 5e-5f
    if (actual == 0.0f) {
        Assertions.assertEquals(expected, actual)
        return
    }
    val ratio = expected / actual
    Assertions.assertTrue( ratio > 1.0f - DELTA && ratio < 1.0f + DELTA,
            "Value $actual not approximately the same as expected $expected!")
}

fun isApproximatelyTheSame(expected: Double, actual: Double) {
    val DELTA = 1e-14
    if (actual == 0.0) {
        Assertions.assertEquals(expected, actual)
        return
    }
    val ratio = expected / actual
    Assertions.assertTrue(ratio > 1.0 - DELTA)
    Assertions.assertTrue(ratio < 1.0 + DELTA)
}

fun isApproximatelyTheSame(expected: Number, actual: Number) {
    val coarse = expected is Float
    val delta = if (coarse) 5e-5 else 1e-14
    if (actual == 0.0) {
        Assertions.assertEquals(expected, actual)
        return
    }
    val ratio = expected.toDouble() / actual.toDouble()
    Assertions.assertTrue(ratio > 1.0 - delta)
    Assertions.assertTrue(ratio < 1.0 + delta)
}

fun equalVectors(expected: VectorValue<*>, actual: VectorValue<*>) {
    Assertions.assertEquals(expected.logicalSize, actual.logicalSize, "Vector sizes differ!")
    for (i in 0 until expected.logicalSize) {
        isApproximatelyTheSame(expected[i].real.value, actual[i].real.value)
        isApproximatelyTheSame(expected[i].imaginary.value, actual[i].imaginary.value)
    }
}

fun arrayFieldVectorFromVectorValue(vector: VectorValue<*>) : ArrayFieldVector<Complex> {
    val size = vector.logicalSize
    val vec = ArrayFieldVector(ComplexField.getInstance(), size)
    for (i in 0 until size) {
        vec.setEntry(i, Complex(vector[i].real.asDouble().value, vector[i].imaginary.asDouble().value))
    }
    return vec
}

fun complex32VectorFromFieldVector(vector: FieldVector<Complex>) : Complex32VectorValue {
    vector as ArrayFieldVector
    val complexs = vector.dataRef.map {
        Complex32Value(it.real, it.imaginary)
    }.toTypedArray()
    return Complex32VectorValue(complexs)
}

fun complex64VectorFromFieldVector(vector: FieldVector<Complex>) : Complex64VectorValue {
    vector as ArrayFieldVector
    val complexs = vector.dataRef.map {
        Complex64Value(it.real, it.imaginary)
    }.toTypedArray()
    return Complex64VectorValue(complexs)
}

fun absFromFromComplexFieldVector(vector: ArrayFieldVector<Complex>) : ArrayRealVector {
    val vec = ArrayRealVector(vector.dimension)
    vector.walkInOptimizedOrder(object : FieldVectorPreservingVisitor<Complex> {
        override fun end() : Complex {return Complex.ZERO}
        override fun start(dimension: Int, start: Int, end: Int) {return}
        override fun visit(index: Int, value: Complex) {vec.setEntry(index, value.abs())}
    })
    return vec
}

fun conjFromFromComplexFieldVector(vector: ArrayFieldVector<Complex>) : ArrayFieldVector<Complex> {
    val vec = ArrayFieldVector(ComplexField.getInstance(), vector.dimension)
    vector.walkInOptimizedOrder(object : FieldVectorPreservingVisitor<Complex> {
        override fun end() : Complex {return Complex.ZERO}
        override fun start(dimension: Int, start: Int, end: Int) {return}
        override fun visit(index: Int, value: Complex) {vec.setEntry(index, value.conjugate())}
    })
    return vec
}
