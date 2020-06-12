package org.vitrivr.cottontail.math.basics

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.util.*

/**
 * Some basic test cases that test for correctness fo [Complex64VectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex64VectorValueTest {

    private val random = SplittableRandom()

    companion object {
        private const val DELTA = 1e-14
        fun isApproximatelyTheSame(expected: Double, actual: Double) {
            if (actual == 0.0) {
                Assertions.assertEquals(expected, actual)
                return
            }
            val ratio = expected / actual
            Assertions.assertTrue(ratio > 1.0 - DELTA)
            Assertions.assertTrue(ratio < 1.0 + DELTA)
        }
    }

    @RepeatedTest(100)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)

        val add: Complex64VectorValue = c1 + c2

        for (i in 0 until size) {
            val addp = c1[i] + c2[i]
            isApproximatelyTheSame(addp.real.value, add[i].real.value)
            isApproximatelyTheSame(addp.imaginary.value, add[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)

        val sub: Complex64VectorValue = c1 - c2

        for (i in 0 until size) {
            val subp = c1[i] - c2[i]
            isApproximatelyTheSame(subp.real.value, sub[i].real.value)
            isApproximatelyTheSame(subp.imaginary.value, sub[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testMult() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)

        val mul: Complex64VectorValue = c1 * c2

        for (i in 0 until size) {
            val mulp = c1[i] * c2[i]
            isApproximatelyTheSame(mulp.real.value, mul[i].real.value)
            isApproximatelyTheSame(mulp.imaginary.value, mul[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)

        val div: Complex64VectorValue = c1 / c2

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
        val c1 = Complex64VectorValue.random(size, this.random)

        val pow: Complex64VectorValue = c1.pow(exp)

        for (i in 0 until size) {
            val powp = c1[i].pow(exp)
            isApproximatelyTheSame(powp.real.value, pow[i].real.value)
            isApproximatelyTheSame(powp.imaginary.value, pow[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testAbs() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)
        val abs: DoubleVectorValue = c1.abs()

        for (i in 0 until size) {
            val absp = c1[i].abs()
            isApproximatelyTheSame(absp.value, abs[i].value)
        }
    }

    @RepeatedTest(100)
    fun testSqrt() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)

        val sqrt: Complex64VectorValue = c1.sqrt()

        for (i in 0 until size) {
            val sqrtp = c1[i].sqrt()
            isApproximatelyTheSame(sqrtp.real.value, sqrt[i].real.value)
            isApproximatelyTheSame(sqrtp.imaginary.value, sqrt[i].imaginary.value)
        }
    }

    @RepeatedTest(100)
    fun testDot() {
        val size = random.nextInt(2048)

        val c1 = Complex64VectorValue.random(size, this.random)
        val c2 = Complex64VectorValue.random(size, this.random)

        val dot: Complex64Value = c1.dot(c2)

        var dotp = Complex64Value(0.0, 0.0)
        for (i in 0 until size) {
            dotp += c1[i] * c2[i].conjugate()
        }

        isApproximatelyTheSame(dotp.real.value, dot.real.value)
        isApproximatelyTheSame(dotp.imaginary.value, dot.imaginary.value)
    }

    @RepeatedTest(100)
    fun testSum() {
        val size = random.nextInt(2048)
        val c1 = Complex64VectorValue.random(size, this.random)
        val sum: Complex64Value = c1.sum()

        var sump = Complex64Value(0.0, 0.0)
        for (i in 0 until size) {
            sump += c1[i]
        }

        isApproximatelyTheSame(sump.real.value, sum.real.value)
        isApproximatelyTheSame(sump.imaginary.value, sum.imaginary.value)
    }
}
