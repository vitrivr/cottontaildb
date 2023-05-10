package org.vitrivr.cottontail.core.types
/**
 * Represents a complex valued [VectorValue] of any primitive type, i.e., a vector whose elements
 * consist of [ComplexValue]s. This  is an abstraction over the existing primitive array types
 * provided by Kotlin. It allows for the advanced type system implemented by Cottontail DB.
 *
 * @see VectorValue
 * @see ComplexValue
 *
 * @version 2.0.0
 * @author Ralph Gasser
 */
interface ComplexVectorValue<T: Number> : VectorValue<T>, Iterable<ComplexValue<T>> {
    /**
     * Returns the i-th entry of  this [ComplexVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): ComplexValue<T>

    /**
     * Returns the subvector of length [length] starting from [start] of this [ComplexVectorValue].
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     * @return The subvector starting at index start containing length elements.
     */
    override fun slice(start: Int, length: Int): ComplexVectorValue<T>

    /**
     * Gets the real part of the i-th entry of this [ComplexVectorValue].
     *
     * @param i The index of the value to return.
     * @return The real component of the i-th entry in this [ComplexVectorValue]
     */
    fun real(i: Int): RealValue<T>

    /**
     * Gets the imaginary part of the i-th entry of this [ComplexVectorValue].
     *
     * @param i The index of the value to return.
     * @return The imaginary component of the i-th entry in this [ComplexVectorValue]
     */
    fun imaginary(i: Int): RealValue<T>

    /**
     * Creates and returns an [Iterator] for the values held by this [ComplexVectorValue].
     */
    override fun iterator(): Iterator<ComplexValue<T>> = object : Iterator<ComplexValue<T>> {
        var index = 0
        override fun hasNext(): Boolean = this.index < this@ComplexVectorValue.logicalSize
        override fun next(): ComplexValue<T> = this@ComplexVectorValue[this.index++]
    }

    /**
     * Creates and returns an exact copy of this [ComplexVectorValue].
     *
     * @return Exact copy of this [ComplexVectorValue].
     */
    override fun copy(): ComplexVectorValue<T>

    /**
     * Creates and returns a copy of this [ComplexVectorValue]'s real components.
     *
     * @return Copy of this [ComplexVectorValue].
     */
    fun copyReal(): RealVectorValue<T>

    /**
     * Creates and returns a copy of this [ComplexVectorValue]'s imaginary components.
     *
     * @return Copy of this [ComplexVectorValue].
     */
    fun copyImaginary(): RealVectorValue<T>

    /**
     * Calculates the element-wise quotient of this and the other [ComplexVectorValue].
     *
     * @param other The [VectorValue] to divide this [ComplexVectorValue] by.
     * @return [ComplexVectorValue] that contains the element-wise quotient of the two input [VectorValue]s
     */
    override fun div(other: VectorValue<*>): ComplexVectorValue<T>

    /**
     * Calculates the element-wise difference of this and the other [VectorValue].
     *
     * @param other The [VectorValue] to subtract from this [ComplexVectorValue].
     * @return [ComplexVectorValue] that contains the element-wise difference of the two input [VectorValue]s
     */
    override operator fun minus(other: VectorValue<*>): ComplexVectorValue<T>

    /**
     * Calculates the element-wise product of this and the other [VectorValue].
     *
     * @param other The [VectorValue] to multiply this [VectorValue] with.
     * @return [ComplexVectorValue] that contains the element-wise product of the two input [VectorValue]s
     */
    override fun times(other: VectorValue<*>): ComplexVectorValue<T>

    /**
     * Calculates the element-wise sum of this and the other [VectorValue].
     *
     * @param other The [VectorValue] to add to this [ComplexVectorValue].
     * @return [ComplexVectorValue] that contains the element-wise sum of the two input [VectorValue]s
     */
    override fun plus(other: VectorValue<*>): ComplexVectorValue<T>

    /**
     * Builds the dot product between this and the other [VectorValue].
     *
     * <strong>Warning:</string> Since the value generated by this function might not fit into the
     * type held by this [ComplexVectorValue], the [ComplexValue] returned by this function might differ.
     *
     * @return Sum of the elements of this [VectorValue].
     */

    override infix fun dot(other: VectorValue<*>): ComplexValue<*>

    override fun plus(other: NumericValue<*>): ComplexVectorValue<T>
    override fun minus(other: NumericValue<*>): ComplexVectorValue<T>
    override fun times(other: NumericValue<*>): ComplexVectorValue<T>
    override fun div(other: NumericValue<*>): ComplexVectorValue<T>
}