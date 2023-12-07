package org.vitrivr.cottontail.dbms.queries.components

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.generators.*
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext

/**
 * Test case that tests for correctness of [ComparisonOperator]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class NumericValueComparisonOperatorTest {
    /**
     * Checks correctness of the [ComparisonOperator.Equal] operator.
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
            with(MissingTuple) {
                /** Assert equality .*/
                Assertions.assertEquals(referenceBoolean, positiveComparisonBoolean)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceBoolean), context.bind(positiveComparisonBoolean)).match())

                Assertions.assertEquals(referenceByte, positiveComparisonByte)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceByte), context.bind(positiveComparisonByte)).match())

                Assertions.assertEquals(referenceShort, positiveComparisonShort)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceShort), context.bind(positiveComparisonShort)).match())

                Assertions.assertEquals(referenceInt, positiveComparisonInt)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceInt), context.bind(positiveComparisonInt)).match())

                Assertions.assertEquals(referenceLong, positiveComparisonLong)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceLong), context.bind(positiveComparisonLong)).match())

                Assertions.assertEquals(referenceFloat, positiveComparisonFloat)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceFloat), context.bind(positiveComparisonFloat)).match())

                Assertions.assertEquals(referenceDouble, positiveComparisonDouble)
                Assertions.assertTrue(ComparisonOperator.Equal(context.bind(referenceDouble), context.bind(positiveComparisonDouble)).match())

                /** Assert inequality .*/
                if (referenceBoolean.value != negativeComparisonBoolean.value) {
                    Assertions.assertNotEquals(referenceBoolean, negativeComparisonBoolean)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceBoolean), context.bind(negativeComparisonBoolean)).match())
                }
                if (referenceByte.value != negativeComparisonByte.value) {
                    Assertions.assertNotEquals(referenceByte, negativeComparisonByte)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceByte), context.bind(negativeComparisonByte)).match())
                }
                if (referenceShort.value != negativeComparisonShort.value) {
                    Assertions.assertNotEquals(referenceShort, negativeComparisonShort)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceShort), context.bind(negativeComparisonShort)).match())
                }
                if (referenceInt.value != negativeComparisonInt.value) {
                    Assertions.assertNotEquals(referenceInt, negativeComparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceInt), context.bind(negativeComparisonInt)).match())
                }
                if (referenceLong.value != negativeComparisonLong.value) {
                    Assertions.assertNotEquals(referenceLong, negativeComparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceLong), context.bind(negativeComparisonLong)).match())
                }
                if (referenceFloat.value != negativeComparisonFloat.value) {
                    Assertions.assertNotEquals(referenceFloat, negativeComparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceFloat), context.bind(negativeComparisonFloat)).match())
                }
                if (referenceDouble.value != negativeComparisonDouble.value) {
                    Assertions.assertNotEquals(referenceDouble, negativeComparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Equal(context.bind(referenceDouble), context.bind(negativeComparisonDouble)).match())
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
            with(MissingTuple) {
                val positiveReference = context.bind(listOf(
                    referenceShort,
                    referenceInt,
                    referenceLong,
                    referenceFloat,
                    referenceDouble,
                    negativeComparisonShort,
                    negativeComparisonInt,
                    negativeComparisonLong,
                    negativeComparisonFloat,
                    negativeComparisonDouble
                ))
                val negativeReference = context.bind(listOf(
                    negativeComparisonShort,
                    negativeComparisonInt,
                    negativeComparisonLong,
                    negativeComparisonFloat,
                    negativeComparisonDouble
                ))

                /** Assert positive IN .*/
                var op1 = ComparisonOperator.In(context.bind(referenceShort), positiveReference)
                op1.prepare()
                Assertions.assertTrue(op1.match())
                op1 = ComparisonOperator.In(context.bind(referenceInt), positiveReference)
                op1.prepare()
                Assertions.assertTrue(op1.match())
                op1 = ComparisonOperator.In(context.bind(referenceLong), positiveReference)
                op1.prepare()
                Assertions.assertTrue(op1.match())
                op1 = ComparisonOperator.In(context.bind(referenceFloat), positiveReference)
                op1.prepare()
                Assertions.assertTrue(op1.match())
                op1 = ComparisonOperator.In(context.bind(referenceDouble), positiveReference)
                op1.prepare()
                Assertions.assertTrue(op1.match())

                /** Assert negative IN .*/
                var op2 = ComparisonOperator.In(context.bind(referenceShort), negativeReference)
                op2.prepare()
                Assertions.assertFalse(op2.match())
                op2 = ComparisonOperator.In(context.bind(referenceInt), negativeReference)
                op2.prepare()
                Assertions.assertFalse(op2.match())
                op2 = ComparisonOperator.In(context.bind(referenceLong), negativeReference)
                Assertions.assertFalse(op2.match())
                op2 = ComparisonOperator.In(context.bind(referenceFloat), negativeReference)
                op2.prepare()
                Assertions.assertFalse(op2.match())
                op2 = ComparisonOperator.In(context.bind(referenceDouble), negativeReference)
                op2.prepare()
                Assertions.assertFalse(op2.match())
            }
        }
    }


    /**
     * Checks correctness of the [ComparisonOperator.Greater] and [ComparisonOperator.Less] operator.
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
            with(MissingTuple) {
                /** Assert inequality (Byte).*/
                if (referenceByte.value > comparisonByte.value) {
                    Assertions.assertNotEquals(referenceByte, comparisonByte)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Greater(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.LessEqual(referenceByteBinding, comparisonByteBinding).match())
                } else if (referenceByte.value < comparisonByte.value) {
                    Assertions.assertNotEquals(referenceByte, comparisonByte)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceByteBinding, comparisonByteBinding).match())
                } else {
                    Assertions.assertEquals(referenceByte, comparisonByte)
                    Assertions.assertTrue(ComparisonOperator.Equal(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceByteBinding, comparisonByteBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceByteBinding, comparisonByteBinding).match())
                }

                /** Assert inequality (Short) .*/
                if (referenceShort.value > comparisonShort.value) {
                    Assertions.assertNotEquals(referenceShort, comparisonShort)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Greater(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.LessEqual(referenceShortBinding, comparisonShortBinding).match())
                } else if (referenceShort.value < comparisonShort.value) {
                    Assertions.assertNotEquals(referenceShort, comparisonShort)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceShortBinding, comparisonShortBinding).match())
                } else {
                    Assertions.assertEquals(referenceShort, comparisonShort)
                    Assertions.assertTrue(ComparisonOperator.Equal(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceShortBinding, comparisonShortBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceShortBinding, comparisonShortBinding).match())
                }

                /** Assert inequality (Int) .*/
                if (referenceInt.value > comparisonInt.value) {
                    Assertions.assertNotEquals(referenceInt, comparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Greater(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.LessEqual(referenceIntBinding, comparisonIntBinding).match())
                } else if (referenceInt.value < comparisonInt.value) {
                    Assertions.assertNotEquals(referenceInt, comparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceIntBinding, comparisonIntBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceInt, comparisonInt)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceIntBinding, comparisonIntBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceIntBinding, comparisonIntBinding).match())
                }

                /** Assert inequality (Long) .*/
                if (referenceLong.value > comparisonLong.value) {
                    Assertions.assertNotEquals(referenceLong, comparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Greater(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.LessEqual(referenceLongBinding, comparisonLongBinding).match())
                } else if (referenceLong.value < comparisonLong.value) {
                    Assertions.assertNotEquals(referenceLong, comparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceLongBinding, comparisonLongBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceLong, comparisonLong)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceLongBinding, comparisonLongBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceLongBinding, comparisonLongBinding).match())
                }

                /** Assert inequality (Float) .*/
                if (referenceFloat.value > comparisonFloat.value) {
                    Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Greater(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.LessEqual(referenceFloatBinding, comparisonFloatBinding).match())
                } else if (referenceFloat.value < comparisonFloat.value) {
                    Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceFloatBinding, comparisonFloatBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceFloat, comparisonFloat)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceFloatBinding, comparisonFloatBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceFloatBinding, comparisonFloatBinding).match())
                }

                /** Assert inequality (Double) .*/
                if (referenceDouble.value > comparisonDouble.value) {
                    Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Greater(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.GreaterEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Less(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.LessEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                } else if (referenceDouble.value < comparisonDouble.value) {
                    Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                } else {
                    Assertions.assertNotEquals(referenceDouble, comparisonDouble)
                    Assertions.assertFalse(ComparisonOperator.Equal(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.Greater(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertFalse(ComparisonOperator.GreaterEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.Less(referenceDoubleBinding, comparisonDoubleBinding).match())
                    Assertions.assertTrue(ComparisonOperator.LessEqual(referenceDoubleBinding, comparisonDoubleBinding).match())
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
            with(MissingTuple) {
                /* Bind values. */
                val referenceInt = IntValueGenerator.random()
                val referenceLong = LongValueGenerator.random()
                val referenceFloat = FloatValueGenerator.random()
                val referenceDouble = DoubleValueGenerator.random()

                val comparisonInt = listOf(
                    IntValue(ValueGenerator.RANDOM.nextInt(Int.MIN_VALUE, referenceInt.value)),
                    IntValue(ValueGenerator.RANDOM.nextInt(referenceInt.value, Int.MAX_VALUE))
                )

                val comparisoLong = listOf(
                    LongValue(ValueGenerator.RANDOM.nextLong(Long.MIN_VALUE, referenceLong.value)),
                    LongValue(ValueGenerator.RANDOM.nextLong(referenceLong.value, Long.MAX_VALUE))
                )
                val comparisonFloat = listOf(
                    FloatValue(ValueGenerator.RANDOM.nextDouble(Double.MIN_VALUE, referenceFloat.value.toDouble())),
                    FloatValue(ValueGenerator.RANDOM.nextDouble(referenceFloat.value.toDouble(), Double.MAX_VALUE))
                )

                val comparisonDouble = listOf(
                    DoubleValue(ValueGenerator.RANDOM.nextDouble(Double.MIN_VALUE, referenceDouble.value)),
                    DoubleValue(ValueGenerator.RANDOM.nextDouble(referenceDouble.value, Double.MAX_VALUE))
                )

                /** Assert BETWEEN .*/
                var op1 = ComparisonOperator.Between(context.bind(referenceInt), context.bind(comparisonInt))
                var op2 = ComparisonOperator.Between(context.bind(referenceInt), context.bind(comparisonInt.reversed()))
                op1.prepare()
                op2.prepare()
                Assertions.assertTrue(op1.match())
                Assertions.assertTrue(op2.match())

                op1 = ComparisonOperator.Between(context.bind(referenceLong), context.bind(comparisoLong))
                op2 = ComparisonOperator.Between(context.bind(referenceLong), context.bind(comparisoLong.reversed()))
                op1.prepare()
                op2.prepare()
                Assertions.assertTrue(op1.match())
                Assertions.assertTrue(op2.match())

                op1 = ComparisonOperator.Between(context.bind(referenceFloat), context.bind(comparisonFloat))
                op2 = ComparisonOperator.Between(context.bind(referenceFloat), context.bind(comparisonFloat.reversed()))
                op1.prepare()
                op2.prepare()
                Assertions.assertTrue(op1.match())
                Assertions.assertTrue(op2.match())

                op1 = ComparisonOperator.Between(context.bind(referenceDouble), context.bind(comparisonDouble))
                op2 = ComparisonOperator.Between(context.bind(referenceDouble), context.bind(comparisonDouble.reversed()))
                op1.prepare()
                op2.prepare()
                Assertions.assertTrue(op1.match())
                Assertions.assertTrue(op2.match())
            }
        }
    }
}