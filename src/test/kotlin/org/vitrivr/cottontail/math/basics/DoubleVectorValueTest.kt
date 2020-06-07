package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.linear.ArrayRealVector
import org.apache.commons.math3.util.MathArrays
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.util.*
import kotlin.math.pow

/**
 * Some basic test cases that test for correctness fo [DoubleVectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class DoubleVectorValueTest {

    private val random = SplittableRandom()

    @RepeatedTest(100)
    fun testAdd() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val add = c1 + c2
        val addp = MathArrays.ebeAdd(c1.data, c2.data)

        for (i in 0 until size) {
            Assertions.assertEquals(addp[i], add[i].value)
        }
    }

    @RepeatedTest(100)
    fun testSub() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val sub = c1 - c2
        val subp = MathArrays.ebeSubtract(c1.data, c2.data)

        for (i in 0 until size) {
            Assertions.assertEquals(subp[i], sub[i].value)
        }
    }

    @RepeatedTest(100)
    fun testMul() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val mul = c1 * c2
        val mulp = MathArrays.ebeMultiply(c1.data, c2.data)

        for (i in 0 until size) {
            Assertions.assertEquals(mulp[i], mul[i].value)
        }
    }

    @RepeatedTest(100)
    fun testDiv() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val div = c1 / c2
        val divp = MathArrays.ebeDivide(c1.data, c2.data)

        for (i in 0 until size) {
            Assertions.assertEquals(divp[i], div[i].value)
        }
    }

    @RepeatedTest(100)
    fun testPow() {
        val size = random.nextInt(2048)
        val exp = random.nextInt(10)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })

        val pow = c1.pow(exp)
        val powp = c1p.map { it.pow(exp) }

        for (i in 0 until size) {
            Assertions.assertEquals(powp.getEntry(i), pow[i].value)
        }
    }

    @RepeatedTest(100)
    fun testSqrt() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })

        val sqrt = c1.sqrt()
        val sqrtp = c1p.map { kotlin.math.sqrt(it) }

        for (i in 0 until size) {
            Assertions.assertEquals(sqrtp.getEntry(i), sqrt[i].value)
        }
    }

    @RepeatedTest(100)
    fun testL1() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val l1 = c1.l1(c2)
        val l1p = MathArrays.distance1(c1.data, c2.data)

        Assertions.assertEquals(l1p, l1.value)
    }

    @RepeatedTest(100)
    fun testL2() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val l2 = c1.l2(c2)
        val l2p = MathArrays.distance(c1.data, c2.data)

        Assertions.assertEquals(l2p, l2.value)
    }

    @RepeatedTest(100)
    fun testDot() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)
        val c2 = DoubleVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(c1.data)
        val c2p = ArrayRealVector(c2.data)

        val dot = c1.dot(c2)
        val dotp = c1p.dotProduct(c2p)

        Assertions.assertEquals(dotp, dot.value)
    }

    @RepeatedTest(100)
    fun testNorm2() {
        val size = random.nextInt(2048)

        val c1 = DoubleVectorValue.random(size, this.random)

        val c1p = ArrayRealVector(c1.data)

        val norm = c1.norm2()
        val normp = c1p.norm

        Assertions.assertEquals(normp, norm.value)
    }
}