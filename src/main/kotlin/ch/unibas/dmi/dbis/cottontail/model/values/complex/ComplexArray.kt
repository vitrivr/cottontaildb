package ch.unibas.dmi.dbis.cottontail.model.values.complex

/**
 * An array of complex numbers.
 * @constructor Creates a new array of the specified [size], with all elements initialized to zero.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class ComplexArray(size: Int) {

    val size: Int = size

    /**
     * Creates a new array of the specified [size], where each element is calculated by calling the specified
     * [init] function. The [init] function returns an array element given its index.
     */
    // TODO inline constructor

    /**
     * Returns the array element at the given [index]. This method can be called using the index operator.
     *
     * If the [index] is out of bounds of this array, throws an [IndexOutOfBoundsException] except in Kotlin/JS
     * where the behavior is unspecified.
     */
    operator fun get(index: Int): Complex {
        // TODO get
        return Complex(0.0f, 0.0f)
    }

    /**
     * Sets the element at the given [index] to the given [value]. This method can be called using the index operator.
     *
     * If the [index] is out of bounds of this array, throws an [IndexOutOfBoundsException] except in Kotlin/JS
     * where the behavior is unspecified.
     */
    operator fun set(index: Int, value: Complex) {
        // TODO set
    }

    /** Returns the number of elements in the array. */
    // TODO size

    /** Creates an iterator over the elements of the array. */
    fun asIterable(): Iterable<Complex>? {
        // TODO iterator
        return emptyList()
    }
}