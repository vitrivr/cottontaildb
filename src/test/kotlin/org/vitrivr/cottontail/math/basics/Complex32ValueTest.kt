package org.vitrivr.cottontail.math.basics

import org.apache.commons.math3.complex.Complex
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.model.values.Complex32Value
import java.util.*

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Complex32ValueTest {

    private val random = SplittableRandom()

    @RepeatedTest(100)
    fun testAdd() {
        val c1 = Complex32Value.random(random)
        val c2 = Complex32Value.random(random)

        val c1p = Complex(c1.real.asDouble().value, c1.imaginary.asDouble().value)
        val c2p = Complex(c2.real.asDouble().value, c2.imaginary.asDouble().value)


        val add = c1 + c2
        val addp = c1p.add(c2p)

        Assertions.assertEquals(addp.real, add.real.asDouble().value)
        Assertions.assertEquals(addp.imaginary, add.imaginary.asDouble().value)
    }
}

