package org.vitrivr.cottontail.storage.serializer.tablets

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.generators.*
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer
import java.util.*

/**
 * A series of test cases that test various flavours of [TabletSerializer]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TabletSerializationTest {
    /** The [SplittableRandom] used to generate random numbers. */
    private val random = SplittableRandom()

    /** The [Tablet] size to test. */
    private val tabletSize = 128

    /**
     * Tests de-/serialization of [AbstractTablet]s of [IntVectorValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testBooleanTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Boolean)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = BooleanValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [ByteValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testByteTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Byte)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = ByteValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [ByteValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testShortTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Short)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = ShortValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [IntValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testIntTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Int)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = IntValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [IntValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testLongTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Long)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = LongValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [FloatValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testFloatTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Float)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = FloatValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [DoubleValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testDoubleTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Double)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = DoubleValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [DoubleValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testDateTabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Date)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = DateValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [DoubleValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testComplex32TabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Complex32)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = Complex32ValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [DoubleValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testComplex64TabletSerialization(compression: Compression) {
        val tablet = Tablet.of(this.tabletSize, Types.Complex64)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = Complex64ValueGenerator.random(this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [IntVectorValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testBooleanVectorTabletSerialization(compression: Compression) {
        val type = Types.BooleanVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = BooleanVectorValueGenerator.random(type.logicalSize, this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [IntVectorValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testIntVectorTabletSerialization(compression: Compression) {
        val type = Types.IntVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = IntVectorValueGenerator.random(type.logicalSize, this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [IntVectorValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testLongVectorTabletSerialization(compression: Compression) {
        val type = Types.LongVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = LongVectorValueGenerator.random(type.logicalSize, this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [FloatVectorValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testFloatVectorTabletSerialization(compression: Compression) {
        val type = Types.FloatVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = FloatVectorValueGenerator.random(type.logicalSize, this.random)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    /**
     * Tests de-/serialization of [AbstractTablet]s of [DoubleVectorValue]s.
     */
    @ParameterizedTest
    @EnumSource(Compression::class)
    fun testDoubleVectorTabletSerialization(compression: Compression) {
        val type = Types.DoubleVector(this.random.nextInt(4, 2048))
        val tablet = Tablet.of(this.tabletSize, type)
        repeat(this.tabletSize) {
            if (this.random.nextBoolean()) {
                tablet[it] = DoubleVectorValueGenerator.random(type.logicalSize, this.random,)
            }
        }

        /* Create tablet and test de-/serialization. */
        this.test(tablet, compression)
    }

    private inline fun <reified T: Value> test(tablet: Tablet<T>, compression: Compression) {
        val serializer: TabletSerializer<T> = SerializerFactory.tablet(tablet.type, tablet.size, compression)

        /* Deserialize tablet. */
        val serialized = serializer.toEntry(tablet)
        val tablet2 = serializer.fromEntry(serialized)

        /* Compare entries. */
        for (i in 0 until tablet.size) {
            if (tablet[i] == null) {
                Assertions.assertTrue(tablet2[i] == null)
            } else {
                Assertions.assertTrue(tablet[i]!!.isEqual(tablet2[i]!!))
            }
        }
    }
}