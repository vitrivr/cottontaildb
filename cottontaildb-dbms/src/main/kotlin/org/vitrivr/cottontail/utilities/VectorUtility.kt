package org.vitrivr.cottontail.utilities

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.generators.*

/**
 * Utility class used to generate a stream of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
object VectorUtility {

    /**
     * Generates a sequence of random [BooleanVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomBoolVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<BooleanVectorValue> = object : Iterator<BooleanVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): BooleanVectorValue {
            this.left -= 1
            return BooleanVectorValueGenerator.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [IntVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomIntVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<IntVectorValue> = object : Iterator<IntVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): IntVectorValue {
            this.left -= 1
            return IntVectorValueGenerator.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [LongVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomLongVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<LongVectorValue> = object : Iterator<LongVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): LongVectorValue {
            this.left -= 1
            return LongVectorValueGenerator.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [FloatVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomFloatVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<FloatVectorValue> = object : Iterator<FloatVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): FloatVectorValue {
            this.left -= 1
            return FloatVectorValueGenerator.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [DoubleVectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomDoubleVectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<DoubleVectorValue> = object : Iterator<DoubleVectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): DoubleVectorValue {
            this.left -= 1
            return DoubleVectorValueGenerator.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [Complex32VectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomComplex32VectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<Complex32VectorValue> = object : Iterator<Complex32VectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): Complex32VectorValue {
            this.left -= 1
            return Complex32VectorValueGenerator.random(size, random)
        }
    }

    /**
     * Generates a sequence of random [Complex64VectorValue] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomComplex64VectorSequence(size: Int, items: Int = Int.MAX_VALUE, random: JDKRandomGenerator = JDKRandomGenerator()): Iterator<Complex64VectorValue> = object : Iterator<Complex64VectorValue> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0
        override fun next(): Complex64VectorValue {
            this.left -= 1
            return Complex64VectorValueGenerator.random(size, random)
        }
    }
}