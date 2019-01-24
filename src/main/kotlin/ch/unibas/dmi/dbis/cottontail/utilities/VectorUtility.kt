package ch.unibas.dmi.dbis.cottontail.utilities

import java.util.*

object VectorUtility {

    /* The random number generator used for vector generation. */
    val random = Random()

    /**
     * Generates a random [FloatArray] of the given size.
     *
     * @param size The size of the random vector.
     */
    fun randomFloatVector(size: Int) : FloatArray {
        val vec = FloatArray(size)
        for (i in 0 until vec.size) {
            vec[i] = random.nextFloat()
        }
        return vec
    }

    /**
     * Generates a sequence of random [FloatArray] of the given size.
     *
     * @param size The size of the random vectors.
     * @param items The number of items to return from the [Iterator]
     */
    fun randomFloatVectorSequence(size: Int, items: Int = Int.MAX_VALUE) : Iterator<FloatArray> = object: Iterator<FloatArray> {
        var left = items
        override fun hasNext(): Boolean = this.left > 0

        override fun next(): FloatArray {
            this.left -= 1
            return randomFloatVector(size)
        }
    }
}