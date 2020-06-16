package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.complex.Complex
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.model.values.*
import java.util.*

/**
 * Some basic test cases that test for correctness fo [_root_ide_package_.org.vitrivr.cottontail.model.values.Complex32VectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex32VectorValueTest {

    private val random = SplittableRandom()

    companion object {
    }

    @RepeatedTest(100)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val add: Complex32VectorValue = c1 + c2
        val add64: Complex32VectorValue = c1 + c264
        val addp = c1p.add(c2p)

        equalVectors(complex32VectorFromFieldVector(addp), add)
        equalVectors(complex32VectorFromFieldVector(addp), add64)
    }

    @RepeatedTest(100)
    fun testAddScalar() {
        val size = random.nextInt(2048)
        val inc = random.nextDouble()

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val add: Complex32VectorValue = c1 + DoubleValue(inc)
        val addp = c1p.mapAdd(Complex(inc))

        equalVectors(complex32VectorFromFieldVector(addp), add)
    }

    @RepeatedTest(100)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val sub: Complex32VectorValue = c1 - c2
        val sub64: Complex32VectorValue = c1 - c264
        val subp = c1p.subtract(c2p)

        equalVectors(complex32VectorFromFieldVector(subp), sub)
        equalVectors(complex32VectorFromFieldVector(subp), sub64)
    }

    @RepeatedTest(100)
    fun testSubScalar() {
        val size = random.nextInt(2048)
        val number = random.nextDouble()

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val sub: Complex32VectorValue = c1 - DoubleValue(number)
        val subp = c1p.mapSubtract(Complex(number))

        equalVectors(complex32VectorFromFieldVector(subp), sub)
    }

    @RepeatedTest(100)
    fun testMult() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val mul: Complex32VectorValue = c1 * c2
        val mul64: Complex32VectorValue = c1 * c264
        val mulp = c1p.ebeMultiply(c2p)

        equalVectors(complex32VectorFromFieldVector(mulp), mul)
        equalVectors(complex32VectorFromFieldVector(mulp), mul64)
    }

    @RepeatedTest(100)
    fun testMultScalar() {
        val size = random.nextInt(2048)
        val fac = random.nextDouble()

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val mult: Complex32VectorValue = c1 * DoubleValue(fac)
        val multp = c1p.mapMultiply(Complex(fac))

        equalVectors(complex32VectorFromFieldVector(multp), mult)
    }

    @RepeatedTest(100)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val div: Complex32VectorValue = c1 / c2
        val div64: Complex32VectorValue = c1 / c264
        val divp = c1p.ebeDivide(c2p)

        equalVectors(complex32VectorFromFieldVector(divp), div)
        equalVectors(complex32VectorFromFieldVector(divp), div64)
    }

    @RepeatedTest(100)
    fun testDivScalar() {
        val size = random.nextInt(2048)
        val number = random.nextDouble()

        val c1 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)

        val div: Complex32VectorValue = c1 / DoubleValue(number)
        val divp = c1p.mapDivide(Complex(number))

        equalVectors(complex32VectorFromFieldVector(divp), div)
    }

    @RepeatedTest(100)
    fun testPow() {
        val size = random.nextInt(2048)
        val exp = random.nextInt(10)
        val c1 = Complex32VectorValue.random(size, this.random)

        val pow: Complex32VectorValue = c1.pow(exp)

        for (i in 0 until size) {
            val powp = c1[i].pow(exp)
            isApproximatelyTheSame(powp.real.value.toFloat(), pow[i].real.value)
            isApproximatelyTheSame(powp.imaginary.value.toFloat(), pow[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testAbs() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val abs: FloatVectorValue = c1.abs()

        for (i in 0 until size) {
            val absp = c1[i].abs()
            isApproximatelyTheSame(absp.value, abs[i].value)
        }
    }

    @RepeatedTest(100)
    fun testSqrt() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)

        val sqrt: Complex32VectorValue = c1.sqrt()

        for (i in 0 until size) {
            val sqrtp = c1[i].sqrt()
            isApproximatelyTheSame(sqrtp.real.value.toFloat(), sqrt[i].real.value)
            isApproximatelyTheSame(sqrtp.imaginary.value.toFloat(), sqrt[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testDot() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)
        val c264 = Complex64VectorValue(c2.data.map { it.toDouble() }.toDoubleArray())

        val dot: Complex32Value = c1.dot(c2)
        val dot64: Complex32Value = c1.dot(c264)

        var dotp = Complex64Value(0.0, 0.0)
        for (i in 0 until size) {
            dotp += c1[i] * c2[i].conjugate()
        }

        isApproximatelyTheSame(dotp.real.value.toFloat(), dot.real.value)
        isApproximatelyTheSame(dotp.imaginary.value.toFloat(), dot.imaginary.value)
        isApproximatelyTheSame(dotp.real.value.toFloat(), dot64.real.value)
        isApproximatelyTheSame(dotp.imaginary.value.toFloat(), dot64.imaginary.value)
    }

    @RepeatedTest(100)
    fun testSum() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val sum: Complex32Value = c1.sum()

        var sump = Complex32Value(0.0f, 0.0f)
        for (i in 0 until size) {
            sump += c1[i]
        }

        isApproximatelyTheSame(sump.real.value, sum.real.value)
        isApproximatelyTheSame(sump.imaginary.value, sum.imaginary.value)
    }

    @RepeatedTest(100)
    fun testL1() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val l1: DoubleValue = c1.l1(c2)
        val l1p = (c1 - c2).abs().sum()
        val l2p2 = absFromFromComplexFieldVector(c1p.subtract(c2p)).l1Norm

        isApproximatelyTheSame(l1p.real.value, l1.real.value.toFloat())
        isApproximatelyTheSame(l1p.imaginary.value, l1.imaginary.value.toFloat())
        isApproximatelyTheSame(l2p2.toFloat(), l1.asFloat().value)
    }

    @RepeatedTest(100)
    fun testL2() {
        val size = random.nextInt(2048)
        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val c1p = arrayFieldVectorFromVectorValue(c1)
        val c2p = arrayFieldVectorFromVectorValue(c2)

        val l2: DoubleValue = c1.l2(c2).asDouble()
        val l2p = (c1 - c2).abs().pow(2).sum().pow(0.5).asComplex32()
        val l2p2 = absFromFromComplexFieldVector(c1p.subtract(c2p)).norm

        isApproximatelyTheSame(l2p.real.value, l2.real.value.toFloat())
        isApproximatelyTheSame(l2p.imaginary.value, l2.imaginary.value.toFloat())
        isApproximatelyTheSame(l2p2.toFloat(), l2.asFloat().value)
    }

    @RepeatedTest(100)
    fun testLp() {
        val size = random.nextInt(2048)
        val p = random.nextInt(2, 10)
        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val lp: DoubleValue = c1.lp(c2, p) as DoubleValue
        val lpp = (c1 - c2).abs().pow(p).sum().pow(1.0 / p)

        isApproximatelyTheSame(lpp.real.value.toFloat(), lp.real.value.toFloat())
        isApproximatelyTheSame(lpp.imaginary.value.toFloat(), lp.imaginary.value.toFloat())
    }

    @Test
    fun testNorm2() {
        val v = Complex32VectorValue(floatArrayOf(.0f, -3.0f, 4.2f, 3.4f, -2.1f, 0.0f))
        isApproximatelyTheSame(6.527633568147036f, v.norm2().asFloat().value)
        val o = Complex32VectorValue(floatArrayOf(.0f, .0f, .0f, .0f, .0f, .0f))
        isApproximatelyTheSame(0.0f, o.norm2().asFloat().value)
    }
}