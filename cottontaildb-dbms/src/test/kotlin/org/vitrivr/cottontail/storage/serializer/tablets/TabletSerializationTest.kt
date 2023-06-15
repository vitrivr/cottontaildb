package org.vitrivr.cottontail.storage.serializer.tablets

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.generators.*
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer
import java.util.ArrayList
import java.util.SplittableRandom

/**
 * A series of test cases that test various flavours of [TabletSerializer]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TabletSerializationTest {

    private val random = SplittableRandom()

    /**
     * Tests de-/serialization of [Tablet]s of [IntVectorValue]s.
     */
    @Test
    fun testBooleanTabletSerialization() {
        val type = Types.Boolean
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                BooleanValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [ByteValue]s.
     */
    @Test
    fun testByteTabletSerialization() {
        val type = Types.Byte
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
        if (this.random.nextBoolean()) {
                ByteValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [ByteValue]s.
     */
    @Test
    fun testShortTabletSerialization() {
        val type = Types.Short
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                ShortValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [IntValue]s.
     */
    @Test
    fun testIntTabletSerialization() {
        val type = Types.Int
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                IntValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [IntValue]s.
     */
    @Test
    fun testLongTabletSerialization() {
        val type = Types.Long
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                LongValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [FloatValue]s.
     */
    @Test
    fun testFloatTabletSerialization() {
        val type = Types.Float
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                FloatValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [DoubleValue]s.
     */
    @Test
    fun testDoubleTabletSerialization() {
        val type = Types.Double
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                DoubleValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [DoubleValue]s.
     */
    @Test
    fun testDateTabletSerialization() {
        val type = Types.Date
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                DateValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [DoubleValue]s.
     */
    @Test
    fun testComplex32TabletSerialization() {
        val type = Types.Complex32
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                Complex32ValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [DoubleValue]s.
     */
    @Test
    fun testComplex64TabletSerialization() {
        val type = Types.Complex64
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                Complex64ValueGenerator.random(this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [IntVectorValue]s.
     */
    @Test
    fun testIntVectorTabletSerialization() {
        val type = Types.IntVector(this.random.nextInt(4, 2048))
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                IntVectorValueGenerator.random(type.logicalSize, this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [IntVectorValue]s.
     */
    @Test
    fun testLongVectorTabletSerialization() {
        val type = Types.LongVector(this.random.nextInt(4, 2048))
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                LongVectorValueGenerator.random(type.logicalSize, this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [FloatVectorValue]s.
     */
    @Test
    fun testFloatVectorTabletSerialization() {
        val type = Types.FloatVector(this.random.nextInt(4, 2048))
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                FloatVectorValueGenerator.random(type.logicalSize, this.random)
            } else {
                null
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(Tablet(type, list))
    }

    /**
     * Tests de-/serialization of [Tablet]s of [DoubleVectorValue]s.
     */
    @Test
    fun testDoubleVectorTabletSerialization() {
        val type = Types.DoubleVector(this.random.nextInt(4, 2048))
        val list = Array<Value?>(TabletSerializer.DEFAULT_SIZE) {
            if (this.random.nextBoolean()) {
                DoubleVectorValueGenerator.random(type.logicalSize, this.random)
            } else {
                null
            }
        }

        /* Create tablet and serialize it. */
        this.test(Tablet(type, list))
    }

    private inline fun <reified T: Value> test(tablet: Tablet<T>) {
        val serializer: TabletSerializer<T> = SerializerFactory.tablet(tablet.type)

        /* Deserialize tablet. */
        val serialized = serializer.toEntry(tablet)
        val tablet2 = serializer.fromEntry(serialized)

        /* Compare entries. */
        for (i in 0 until TabletSerializer.DEFAULT_SIZE) {
            if (tablet[i] == null) {
                Assertions.assertTrue(tablet2[i] == null)
            } else {
                Assertions.assertTrue(tablet[i]!!.isEqual(tablet2[i]!!))
            }
        }
    }
}