package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.linear.ArrayRealVector
import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.generators.DoubleVectorValueGenerator
import org.vitrivr.cottontail.math.isApproximatelyTheSame
import kotlin.math.pow

/**
 * Some basic test cases that test for correctness of [DoubleVectorValue] arithmetic operations.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DoubleVectorValueTest {

    private val random = JDKRandomGenerator()

    @RepeatedTest(25)
    fun testAdd() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it]})

        val add = c1 + c2
        val addp = c1p.add(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(addp.getEntry(i), add[i].value)
        }
    }

    @RepeatedTest(25)
    fun testSub() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it] })

        val sub = c1 - c2
        val subp = c1p.subtract(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(subp.getEntry(i), sub[i].value)
        }
    }

    @RepeatedTest(25)
    fun testMul() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it] })

        val mul = c1 * c2
        val mulp = c1p.ebeMultiply(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(mulp.getEntry(i), mul[i].value)
        }
    }

    @RepeatedTest(25)
    fun testDiv() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it] })

        val div = c1 / c2
        val divp = c1p.ebeDivide(c2p)

        for (i in 0 until size) {
            isApproximatelyTheSame(divp.getEntry(i), div[i].value)
        }
    }

    @RepeatedTest(25)
    fun testPow() {
        val size = this.random.nextInt(2048)
        val exp = this.random.nextInt(10)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })

        val pow = c1.pow(exp)
        val powp = c1p.map { it.pow(exp) }

        for (i in 0 until size) {
            isApproximatelyTheSame(powp.getEntry(i), pow[i].value)
        }
    }

    @RepeatedTest(25)
    fun testSqrt() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })

        val sqrt = c1.sqrt()
        val sqrtp = c1p.map { kotlin.math.sqrt(it) }

        for (i in 0 until size) {
            isApproximatelyTheSame(sqrtp.getEntry(i), sqrt[i].value)
        }
    }

    @RepeatedTest(25)
    fun testL1() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it] })

        val l1 = c1.l1(c2)
        val l1p = c1p.getL1Distance(c2p)

        isApproximatelyTheSame(l1p, l1.value)
    }

    @RepeatedTest(10)
    fun testL2() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it] })

        val l2 = c1.l2(c2)
        val l2p = c1p.getDistance(c2p)

        isApproximatelyTheSame(l2p, l2.value)
    }

    @RepeatedTest(10)
    fun testDot() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)
        val c2 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })
        val c2p = ArrayRealVector(DoubleArray(c2.data.size) { c2.data[it] })

        val dot = c1.dot(c2)
        val dotp = c1p.dotProduct(c2p)

        isApproximatelyTheSame(dotp, dot.value)
    }

    @RepeatedTest(10)
    fun testNorm2() {
        val size = this.random.nextInt(2048)

        val c1 = DoubleVectorValueGenerator.random(size, this.random)

        val c1p = ArrayRealVector(DoubleArray(c1.data.size) { c1.data[it] })

        val norm = c1.norm2()
        val normp = c1p.norm

        isApproximatelyTheSame(normp, norm.value)
    }
}