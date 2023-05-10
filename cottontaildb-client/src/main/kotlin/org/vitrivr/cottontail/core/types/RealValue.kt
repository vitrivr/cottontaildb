package org.vitrivr.cottontail.core.types

/**
 * Represent a real (i.e., non-complex) value containing a primitive type such as [Short], [Int],
 * [Long], [Float] or [Double]. This is an abstraction over the existing primitive types provided
 * by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @version 2.0.0
 * @author Ralph Gasser
 */
interface RealValue<T : Number>: NumericValue<T> {
    companion object {
        /**
         * Returns the smaller of two [RealValue]s.
         *
         * @param a The first [RealValue] to compare.
         * @param b The second [RealValue] to compare.
         * @return The smallest of the tow [RealValue]s
         */
        fun min(a: RealValue<*>, b: RealValue<*>): RealValue<*> = if (a < b) { a } else { b }

        /**
         * Returns the larger of two [RealValue]s.
         *
         * @param a The first [RealValue] to compare.
         * @param b The second [RealValue] to comapre.
         * @return The largest of the tow [RealValue]s
         */
        fun max(a: RealValue<*>, b: RealValue<*>): RealValue<*> = if (a > b) { a } else { b }
    }
}