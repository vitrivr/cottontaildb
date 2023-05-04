package org.vitrivr.cottontail.dbms.queries.components

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.generators.*
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext

/**
 * Test case that tests for correctness of [ComparisonOperator]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class NumericValueComparisonOperatorTest {
    /**
     * Checks correctness of the [ComparisonOperator.IsNull] operator.
     */
    @RepeatedTest(100)
    fun checkIsNull() {
        val context = DefaultBindingContext()
        with(context) {
            with(MissingRecord) {
                val referenceBoolean = BooleanValueGenerator.random()
                val referenceByte = ByteValueGenerator.random()
                val referenceShort = ShortValueGenerator.random()
                val referenceInt = IntValueGenerator.random()
                val referenceLong = LongValueGenerator.random()
                val referenceFloat = FloatValueGenerator.random()
                val referenceDouble = DoubleValueGenerator.random()

                /** Assert ISNULL. */
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceBoolean)).match())
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceByte)).match())
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceShort)).match())
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceInt)).match())
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceLong)).match())
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceFloat)).match())
                Assertions.assertFalse(ComparisonOperator.IsNull(context.bind(referenceDouble)).match())
                Assertions.assertTrue(ComparisonOperator.IsNull(context.bindNull(Types.Byte)).match())
                Assertions.assertTrue(ComparisonOperator.IsNull(context.bindNull(Types.Int)).match())
                Assertions.assertTrue(ComparisonOperator.IsNull(context.bindNull(Types.Long)).match())
                Assertions.assertTrue(ComparisonOperator.IsNull(context.bindNull(Types.Float)).match())
                Assertions.assertTrue(ComparisonOperator.IsNull(context.bindNull(Types.Double)).match())
            }
        }
    }

    /**
     * Checks correctness of the [ComparisonOperator.Binary.Equal] operator.
     */
    @RepeatedTest(100)
    fun checkEqual() {
        val referenceBoolean = BooleanValueGenerator.random()
        val referenceByte = ByteValueGenerator.random()
        val referenceShort = ShortValueGenerator.random()
        val referenceInt = IntValueGenerator.random()
        val referenceLong = LongValueGenerator.random()
        val referenceFloat = FloatValueGenerator.random()
        val referenceDouble = DoubleValueGenerator.random()

        val positiveComparisonBoolean = BooleanValue(referenceBoolean.value)
        val positiveComparisonByte = ByteValue(referenceByte.value)
        val positiveComparisonShort = ShortValue(referenceShort.value)
        val positiveComparisonInt = IntValue(referenceInt.value)
        val positiveComparisonLong = LongValue(referenceLong.value)
        val positiveComparisonFloat = FloatValue(referenceFloat.value)
        val positiveComparisonDouble = DoubleValue(referenceDouble.value)

        val negativeComparisonBoolean = BooleanValueGenerator.random()
        val negativeComparisonByte = ByteValueGenerator.random()
        val negativeComparisonShort = ShortValueGenerator.random()
        val negativeComparisonInt = IntValueGenerator.random()
        val negativeComparisonLong = LongValueGenerator.random()
        val negativeComparisonFloat = FloatValueGenerator.random()
        val negativeComparisonDouble = DoubleValueGenerator.random()

        val context = DefaultBindingContext()

        with(context) {
            with(MissingRecord) {
                /** Assert equality .*/
                Assertions.assertEquals(referenceBoolean, positiveComparisonBoolean)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceBoolean), context.bind(positiveComparisonBoolean)).match())

                Assertions.assertEquals(referenceByte, positiveComparisonByte)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceByte), context.bind(positiveComparisonByte)).match())

                Assertions.assertEquals(referenceShort, positiveComparisonShort)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceShort), context.bind(positiveComparisonShort)).match())

                Assertions.assertEquals(referenceInt, positiveComparisonInt)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceInt), context.bind(positiveComparisonInt)).match())

                Assertions.assertEquals(referenceLong, positiveComparisonLong)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceLong), context.bind(positiveComparisonLong)).match())

                Assertions.assertEquals(referenceFloat, positiveComparisonFloat)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceFloat), context.bind(positiveComparisonFloat)).match())

                Assertions.assertEquals(referenceDouble, positiveComparisonDouble)
                Assertions.assertTrue(ComparisonOperator.Binary.Equal(context.bind(referenceDouble), context.bind(positiveComparisonDouble)).match())

                /** Assert inequality .*/
                if (referenceBoolean.value != negativeComparisonBoolean.value) {
                    Assertions.assertNotEquals(referenceBoolean, negativeComparisonBoolean)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceBoolean), context.bind(negativeComparisonBoolean)).match())
                }
                if (referenceByte.value != negativeComparisonByte.value) {
                    Assertions.assertNotEquals(referenceByte, negativeComparisonByte)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceByte), context.bind(negativeComparisonByte)).match())
                }
                if (referenceShort.value != negativeComparisonShort.value) {
                    Assertions.assertNotEquals(referenceShort, negativeComparisonShort)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceShort), context.bind(negativeComparisonShort)).match())
                }
                if (referenceInt.value != negativeComparisonInt.value) {
                    Assertions.assertNotEquals(referenceInt, negativeComparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceInt), context.bind(negativeComparisonInt)).match())
                }
                if (referenceLong.value != negativeComparisonLong.value) {
                    Assertions.assertNotEquals(referenceLong, negativeComparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceLong), context.bind(negativeComparisonLong)).match())
                }
                if (referenceFloat.value != negativeComparisonFloat.value) {
                    Assertions.assertNotEquals(referenceFloat, negativeComparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceFloat), context.bind(negativeComparisonFloat)).match())
                }
                if (referenceDouble.value != negativeComparisonDouble.value) {
                    Assertions.assertNotEquals(referenceDouble, negativeComparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(context.bind(referenceDouble), context.bind(negativeComparisonDouble)).match())
                }
            }
        }
    }

    /**
     * Checks correctness of the [ComparisonOperator.In] operator.
     */
    @RepeatedTest(100)
    fun checkIn() {
        val referenceShort = ShortValueGenerator.random()
        val referenceInt = IntValueGenerator.random()
        val referenceLong = LongValueGenerator.random()
        val referenceFloat = FloatValueGenerator.random()
        val referenceDouble = DoubleValueGenerator.random()

        val negativeComparisonShort = ShortValueGenerator.random()
        val negativeComparisonInt = IntValueGenerator.random()
        val negativeComparisonLong = LongValueGenerator.random()
        val negativeComparisonFloat = FloatValueGenerator.random()
        val negativeComparisonDouble = DoubleValueGenerator.random()

        val context = DefaultBindingContext()
        with(context) {
            with(MissingRecord) {
                val positiveReference = mutableListOf(
                    context.bind(referenceShort),
                    context.bind(referenceInt),
                    context.bind(referenceLong),
                    context.bind(referenceFloat),
                    context.bind(referenceDouble),
                    context.bind(negativeComparisonShort),
                    context.bind(negativeComparisonInt),
                    context.bind(negativeComparisonLong),
                    context.bind(negativeComparisonFloat),
                    context.bind(negativeComparisonDouble)
                )
                val negativeReference = mutableListOf(
                    context.bind(negativeComparisonShort),
                    context.bind(negativeComparisonInt),
                    context.bind(negativeComparisonLong),
                    context.bind(negativeComparisonFloat),
                    context.bind(negativeComparisonDouble)
                )

                /** Assert positive IN .*/
                Assertions.assertTrue(ComparisonOperator.In(context.bind(referenceShort), positiveReference).match())
                Assertions.assertTrue(ComparisonOperator.In(context.bind(referenceInt), positiveReference).match())
                Assertions.assertTrue(ComparisonOperator.In(context.bind(referenceLong), positiveReference).match())
                Assertions.assertTrue(ComparisonOperator.In(context.bind(referenceFloat), positiveReference).match())
                Assertions.assertTrue(ComparisonOperator.In(context.bind(referenceDouble), positiveReference).match())

                /** Assert negative IN .*/
                Assertions.assertFalse(ComparisonOperator.In(context.bind(referenceShort), negativeReference).match())
                Assertions.assertFalse(ComparisonOperator.In(context.bind(referenceInt), negativeReference).match())
                Assertions.assertFalse(ComparisonOperator.In(context.bind(referenceLong), negativeReference).match())
                Assertions.assertFalse(ComparisonOperator.In(context.bind(referenceFloat), negativeReference).match())
                Assertions.assertFalse(ComparisonOperator.In(context.bind(referenceDouble), negativeReference).match())
            }
        }
    }


    /**
     * Checks correctness of the [ComparisonOperator.Binary.Greater] and [ComparisonOperator.Binary.Less] operator.
     */
    @RepeatedTest(100)
    fun checkGreaterOrLess() {
        val referenceByte = ByteValueGenerator.random()
        val referenceShort = ShortValueGenerator.random()
        val referenceInt = IntValueGenerator.random()
        val referenceLong = LongValueGenerator.random()
        val referenceFloat = FloatValueGenerator.random()
        val referenceDouble = DoubleValueGenerator.random()

        val comparisonByte = ByteValueGenerator.random()
        val comparisonShort = ShortValueGenerator.random()
        val comparisonInt = IntValueGenerator.random()
        val comparisonLong = LongValueGenerator.random()
        val comparisonFloat = FloatValueGenerator.random()
        val comparisonDouble = DoubleValueGenerator.random()

        val context = DefaultBindingContext()

        val referenceByteBinding = context.bind(referenceByte)
        val referenceShortBinding = context.bind(referenceShort)
        val referenceIntBinding = context.bind(referenceInt)
        val referenceLongBinding = context.bind(referenceLong)
        val referenceFloatBinding = context.bind(referenceFloat)
        val referenceDoubleBinding = context.bind(referenceDouble)

        val comparisonByteBinding = context.bind(comparisonByte)
        val comparisonShortBinding = context.bind(comparisonShort)
        val comparisonIntBinding = context.bind(comparisonInt)
        val comparisonLongBinding = context.bind(comparisonLong)
        val comparisonFloatBinding = context.bind(comparisonFloat)
        val comparisonDoubleBinding = context.bind(comparisonDouble)
        with(context) {
            with(MissingRecord) {
                /** Assert inequality (Byte).*/
                if (referenceByte.value > comparisonByte.value) {
                    Assertions.assertNotEquals(referenceByte, comparisonByte)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Greater(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(referenceByteBinding, comparisonByteBinding).match())
                } else if (referenceByte.value < comparisonByte.value) {
                    Assertions.assertNotEquals(referenceByte, comparisonByte)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceByteBinding, comparisonByteBinding).match())
                } else {
                    Assertions.assertEquals(referenceByte, comparisonByte)
                    Assertions.assertTrue(ComparisonOperator.Binary.Equal(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceByteBinding, comparisonByteBinding).match())
                }

                /** Assert inequality (Short) .*/
                if (referenceShort.value > comparisonShort.value) {
                    Assertions.assertNotEquals(referenceShort, comparisonShort)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Greater(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(referenceShortBinding, comparisonShortBinding).match())
                } else if (referenceShort.value < comparisonShort.value) {
                    Assertions.assertNotEquals(referenceShort, comparisonShort)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceShortBinding, comparisonShortBinding).match())
                } else {
                    Assertions.assertEquals(referenceShort, comparisonShort)
                    Assertions.assertTrue(ComparisonOperator.Binary.Equal(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceShortBinding, comparisonShortBinding).match())
                }

                /** Assert inequality (Int) .*/
                if (referenceInt.value > comparisonInt.value) {
                    Assertions.assertNotEquals(referenceInt, comparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Greater(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(referenceIntBinding, comparisonIntBinding).match())
                } else if (referenceInt.value < comparisonInt.value) {
                    Assertions.assertNotEquals(referenceInt, comparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceIntBinding, comparisonIntBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceInt, comparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceIntBinding, comparisonIntBinding).match())
                }

                /** Assert inequality (Long) .*/
                if (referenceLong.value > comparisonLong.value) {
                    Assertions.assertNotEquals(referenceLong, comparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Greater(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(referenceLongBinding, comparisonLongBinding).match())
                } else if (referenceLong.value < comparisonLong.value) {
                    Assertions.assertNotEquals(referenceLong, comparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceLongBinding, comparisonLongBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceLong, comparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceLongBinding, comparisonLongBinding).match())
                }

                /** Assert inequality (Float) .*/
                if (referenceFloat.value > comparisonFloat.value) {
                    Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Greater(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(referenceFloatBinding, comparisonFloatBinding).match())
                } else if (referenceFloat.value < comparisonFloat.value) {
                    Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceFloatBinding, comparisonFloatBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceFloatBinding, comparisonFloatBinding).match())
                }

                /** Assert inequality (Double) .*/
                if (referenceDouble.value > comparisonDouble.value) {
                    Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Greater(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.GreaterEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Less(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.LessEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                } else if (referenceDouble.value < comparisonDouble.value) {
                    Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Binary.Equal(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.Greater(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Binary.GreaterEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.Less(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Binary.LessEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                }
            }
        }
    }

    /**
     * Checks correctness of the [ComparisonOperator.Between] operator.
     */
    @RepeatedTest(100)
    fun checkBetween() {
        val context = DefaultBindingContext()

        with(context) {
            with(MissingRecord) {
                /* Bind values. */
                val referenceInt = IntValueGenerator.random()
                val referenceLong = LongValueGenerator.random()
                val referenceFloat = FloatValueGenerator.random()
                val referenceDouble = DoubleValueGenerator.random()

                val comparisonLowerInt = context.bind(IntValue(ValueGenerator.RANDOM.nextInt(Int.MIN_VALUE, referenceInt.value)))
                val comparisonLowerLong = context.bind(LongValue(ValueGenerator.RANDOM.nextLong(Long.MIN_VALUE, referenceLong.value)))
                val comparisonLowerFloat = context.bind(FloatValue(ValueGenerator.RANDOM.nextDouble(Double.MIN_VALUE, referenceFloat.value.toDouble())))
                val comparisonLowerDouble = context.bind(DoubleValue(ValueGenerator.RANDOM.nextDouble(Double.MIN_VALUE, referenceDouble.value)))

                val comparisonUpperInt = context.bind(IntValue(ValueGenerator.RANDOM.nextInt(referenceInt.value, Int.MAX_VALUE)))
                val comparisonUpperLong = context.bind(LongValue(ValueGenerator.RANDOM.nextLong(referenceLong.value, Long.MAX_VALUE)))
                val comparisonUpperFloat = context.bind(FloatValue(ValueGenerator.RANDOM.nextDouble(referenceFloat.value.toDouble(), Double.MAX_VALUE)))
                val comparisonUpperDouble = context.bind(DoubleValue(ValueGenerator.RANDOM.nextDouble(referenceDouble.value, Double.MAX_VALUE)))

                /** Assert BETWEEN .*/
                Assertions.assertTrue(ComparisonOperator.Between(context.bind(referenceInt), comparisonLowerInt, comparisonUpperInt).match())
                Assertions.assertFalse(ComparisonOperator.Between(context.bind(referenceInt), comparisonUpperInt, comparisonLowerInt).match())

                Assertions.assertTrue(ComparisonOperator.Between(context.bind(referenceLong), comparisonLowerLong, comparisonUpperLong).match())
                Assertions.assertFalse(ComparisonOperator.Between(context.bind(referenceLong), comparisonUpperLong, comparisonLowerLong).match())

                Assertions.assertTrue(ComparisonOperator.Between(context.bind(referenceFloat), comparisonLowerFloat, comparisonUpperFloat).match())
                Assertions.assertFalse(ComparisonOperator.Between(context.bind(referenceFloat), comparisonUpperFloat, comparisonLowerFloat).match())

                Assertions.assertTrue(ComparisonOperator.Between(context.bind(referenceDouble), comparisonLowerDouble, comparisonUpperDouble).match())
                Assertions.assertFalse(ComparisonOperator.Between(context.bind(referenceDouble), comparisonUpperDouble, comparisonLowerDouble).match())
            }
        }
    }
}