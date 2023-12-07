package org.vitrivr.cottontail.storage.serializer.tablets

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.generators.*
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet
import java.util.*

/**
 * A series of test cases that test the ability of [AbstractTablet]s to hold and retrieve [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TabletTest {
    /** The [SplittableRandom] used to generate random numbers. */
    private val random = SplittableRandom()

    /** The size of the [AbstractTablet]*/
    private val tabletSize = 128

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Int]s.
     */
    @Test
    fun testBooleanTablet() {
        val type = Types.Boolean
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val value = BooleanValueGenerator.random(this.random)
            tablet[it] = value
            value
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }
    
    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Int]s.
     */
    @Test
    fun testByteTablet() {
        val type = Types.Byte
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector = ByteValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Int]s.
     */
    @Test
    fun testShortTablet() {
        val type = Types.Short
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  ShortValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Int]s.
     */
    @Test
    fun testIntTablet() {
        val type = Types.Int
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  IntValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Long]s.
     */
    @Test
    fun testLongTablet() {
        val type = Types.Long
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  LongValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Long]s.
     */
    @Test
    fun testFloatTablet() {
        val type = Types.Float
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  FloatValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }


    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Long]s.
     */
    @Test
    fun testDoubleTablet() {
        val type = Types.Double
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  DoubleValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Long]s.
     */
    @Test
    fun testComplex32Tablet() {
        val type = Types.Complex32
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  Complex32ValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.Long]s.
     */
    @Test
    fun testComplex64Tablet() {
        val type = Types.Complex64
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  Complex64ValueGenerator.random(this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.IntVector]s.
     */
    @Test
    fun testBooleanVectorTablet() {
        val type = Types.BooleanVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  BooleanVectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.IntVector]s.
     */
    @Test
    fun testIntVectorTablet() {
        val type = Types.IntVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  IntVectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.LongVector]s.
     */
    @Test
    fun testLongVectorTablet() {
        val type = Types.LongVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  LongVectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.FloatVector]s.
     */
    @Test
    fun testFloatVectorTablet() {
        val type = Types.FloatVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  FloatVectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] =vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.DoubleVector]s.
     */
    @Test
    fun testDoubleVectorTablet() {
        val type = Types.DoubleVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  DoubleVectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.DoubleVector]s.
     */
    @Test
    fun testComplex32VectorTablet() {
        val type = Types.Complex32Vector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  Complex32VectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of type [Types.DoubleVector]s.
     */
    @Test
    fun testComplex64VectorTablet() {
        val type = Types.Complex64Vector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        val list = (0 until this.tabletSize).map {
            val vector =  Complex64VectorValueGenerator.random(type.logicalSize, this.random)
            tablet[it] = vector
            vector
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, list)
    }

    /**
     * Actual test logic.
     */
    private inline fun <reified T: Value> test(tablet: Tablet<T>, list: List<T?>) {
        for ((i, v) in list.withIndex()) {
            if (v == null) {
                Assertions.assertNull(tablet[i])
            } else {
                Assertions.assertTrue(tablet[i]!!.isEqual(v))
            }
        }
    }
}