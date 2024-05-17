package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import java.util.*
import java.util.random.RandomGenerator

/**
 * A class that can be used to generate a certain type of [ValueGenerator].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface ValueGenerator<T: Value> {
    companion object {
        /** Internal [RandomGenerator] instance used for generation of random [Value]s. */
        @JvmStatic
        val RANDOM: RandomGenerator = SplittableRandom()

        /**
         * Generates and returns a [ValueGenerator] for the given [Types].
         *
         * @return [ValueGenerator]
         */
        fun Types<*>.random(): ValueGenerator<*> = when(this) {
            Types.Boolean -> BooleanValueGenerator
            Types.ByteString -> ByteStringValueGenerator
            Types.Date -> DateValueGenerator
            Types.Byte -> ByteValueGenerator
            Types.Complex32 -> Complex32ValueGenerator
            Types.Complex64 -> Complex64ValueGenerator
            Types.Double -> DoubleValueGenerator
            Types.Float -> FloatValueGenerator
            Types.Int -> IntValueGenerator
            Types.Long -> LongValueGenerator
            Types.Short -> ShortValueGenerator
            Types.String -> StringValueGenerator
            Types.Uuid -> UuidValueGenerator
            is Types.BooleanVector -> BooleanVectorValueGenerator
            is Types.Complex32Vector -> Complex32ValueGenerator
            is Types.Complex64Vector -> Complex64ValueGenerator
            is Types.DoubleVector -> DoubleVectorValueGenerator
            is Types.FloatVector -> FloatVectorValueGenerator
            is Types.HalfVector -> HalfVectorValueGenerator
            is Types.IntVector -> IntVectorValueGenerator
            is Types.LongVector -> LongVectorValueGenerator
            is Types.ShortVector -> ShortVectorValueGenerator
        }
    }

    /**
     * Generates a new, random [Value] of type [T].
     *
     * @param rnd The [RandomGenerator] to use.
     * @return [T]
     */
    fun random(rnd: RandomGenerator = RANDOM): T
}