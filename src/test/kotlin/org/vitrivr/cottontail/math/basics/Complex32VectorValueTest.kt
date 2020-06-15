package org.vitrivr.cottontail.math.basics

import org.junit.jupiter.api.Assertions
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
        private const val DELTA = 1e-6f
        fun isApproximatelyTheSame(expected: Float, actual: Float) {
            if (actual == 0.0f) {
                Assertions.assertEquals(expected, actual)
                return
            }
            val ratio = expected / actual
            Assertions.assertTrue(ratio > 1.0f - DELTA)
            Assertions.assertTrue(ratio < 1.0f + DELTA)
        }
    }

    @RepeatedTest(100)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val add: Complex32VectorValue = c1 + c2

        for (i in 0 until size) {
            val addp = c1[i] + c2[i]
            isApproximatelyTheSame(addp.real.value, add[i].real.value)
            isApproximatelyTheSame(addp.imaginary.value, add[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val sub: Complex32VectorValue = c1 - c2

        for (i in 0 until size) {
            val subp = c1[i] - c2[i]
            isApproximatelyTheSame(subp.real.value, sub[i].real.value)
            isApproximatelyTheSame(subp.imaginary.value, sub[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testMult() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val mul: Complex32VectorValue = c1 * c2

        for (i in 0 until size) {
            val mulp = c1[i] * c2[i]
            isApproximatelyTheSame(mulp.real.value, mul[i].real.value)
            isApproximatelyTheSame(mulp.imaginary.value, mul[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = Complex32VectorValue.random(size, this.random)
        val c2 = Complex32VectorValue.random(size, this.random)

        val div: Complex32VectorValue = c1 / c2

        for (i in 0 until size) {
            val divp = c1[i] / c2[i]
            isApproximatelyTheSame(divp.real.value, div[i].real.value)
            isApproximatelyTheSame(divp.imaginary.value, div[i].imaginary.value)
        }
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

        val dot: Complex32Value = c1.dot(c2)

        var dotp = Complex64Value(0.0, 0.0)
        for (i in 0 until size) {
            dotp += c1[i] * c2[i].conjugate()
        }

        isApproximatelyTheSame(dotp.real.value.toFloat(), dot.real.value)
        isApproximatelyTheSame(dotp.imaginary.value.toFloat(), dot.imaginary.value)
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

        val lp: DoubleValue = c1.l1(c2)
        val lpp = (c1 - c2).abs().sum()

        isApproximatelyTheSame(lpp.real.value, lp.real.value.toFloat())
        isApproximatelyTheSame(lpp.imaginary.value, lp.imaginary.value.toFloat())
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