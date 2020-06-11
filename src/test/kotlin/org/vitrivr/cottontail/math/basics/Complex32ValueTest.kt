package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.complex.Complex
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.FloatValue
import java.util.*

/**
 * Some basic test cases that test for correctness fo [Complex32Value] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex32ValueTest {


    companion object {
        private const val DELTA = 1e-4f
        fun isCorrect(expected: Complex, actual: Complex32Value) {
            val r_ratio = expected.real.toFloat() / actual.real.value
            val i_ratio = expected.imaginary.toFloat() / actual.imaginary.value

            println("r-ratio: $r_ratio, i-ratio: $i_ratio")

            Assertions.assertTrue(r_ratio > 1.0f - DELTA)
            Assertions.assertTrue(r_ratio < 1.0f + DELTA)
            Assertions.assertTrue(i_ratio > 1.0f - DELTA)
            Assertions.assertTrue(i_ratio < 1.0f + DELTA)
        }
    }

    private val random = SplittableRandom()

    @RepeatedTest(100)
    fun testAdd() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)


        val add = c1 + c2
        val addp = c1p.add(c2p)

        isCorrect(addp, add)

    }

    @RepeatedTest(100)
    fun testMinus() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)

        val sub = c1 - c2
        val subp = c1p.subtract(c2p)

        isCorrect(subp, sub)
    }

    @RepeatedTest(100)
    fun testMultiply() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)


        val mult = c1 * c2
        val multp = c1p.multiply(c2p)

        isCorrect(multp, mult)
    }

    @RepeatedTest(100)
    fun testMultiplyReal() {
        val c1 = Complex32Value.random(random)
        val c2 = FloatValue.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = c2.value


        val mult = c1 * c2
        val multp = c1p.multiply(c2p.toDouble())

        isCorrect(multp, mult)
    }

    @RepeatedTest(100)
    fun testDivision() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)

        val div = c1 / c2
        val divp = c1p.divide(c2p)

        isCorrect(divp, div)
    }

    @RepeatedTest(100)
    fun testDivReal() {
        val c1 = Complex32Value.random(random)
        val c2 = FloatValue.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = c2.value

        val div = c1 / c2
        val divp = c1p.divide(c2p.toDouble())

        isCorrect(divp, div)
    }

    @RepeatedTest(100)
    fun testInverse() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val inv = c1.inverse()
        val invp = c1p.reciprocal()

        isCorrect(invp, inv)
    }

    @RepeatedTest(100)
    fun testConjugate() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val conj = c1.conjugate()
        val conjp = c1p.conjugate()

        isCorrect(conjp, conj)
    }

    @RepeatedTest(100)
    fun testExp() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val exp = c1.exp().asComplex32()
        val expp = c1p.exp()

        isCorrect(expp, exp)
    }

    @RepeatedTest(100)
    fun testLn() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val ln = c1.ln().asComplex32()
        val lnp = c1p.log()

        isCorrect(lnp, ln)
    }


    @RepeatedTest(100)
    fun testPow() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val e = random.nextDouble()

        val pow = c1.pow(e).asComplex32()
        val powp = c1p.pow(e)

        isCorrect(powp, pow)
    }

    @RepeatedTest(100)
    fun testSqrt() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)


        val sqrt = c1.sqrt().asComplex32()
        val sqrtp = c1p.sqrt()

        isCorrect(sqrtp, sqrt)
    }

    @RepeatedTest(100)
    fun testCos() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val cos = c1.cos().asComplex32()
        val cosp = c1p.cos()

        isCorrect(cosp, cos)
    }

    @RepeatedTest(100)
    fun testSin() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val sin = c1.sin().asComplex32()
        val sinp = c1p.sin()

        isCorrect(sinp, sin)
    }

    @RepeatedTest(100)
    fun testTan() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val tan = c1.tan().asComplex32()
        val tanp = c1p.tan()

        isCorrect(tanp, tan)
    }

    @RepeatedTest(100)
    fun testAtan() {
        val c1 = Complex32Value.random(random)
        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)

        val atan = c1.atan().asComplex32()
        val atanp = c1p.atan()

        isCorrect(atanp, atan)
    }

    @RepeatedTest(100)
    fun testAddConjugate() {
        val c1 = Complex32Value.random(random)
        val c2 = c1.conjugate()

        val add = c1 + c2

        Assertions.assertEquals(c1.real + c2.real, add.real)
        Assertions.assertEquals(0.0f, add.imaginary.value)
    }
}