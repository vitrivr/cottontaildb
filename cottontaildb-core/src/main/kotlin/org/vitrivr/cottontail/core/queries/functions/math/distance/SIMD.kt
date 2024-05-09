package org.vitrivr.cottontail.core.queries.functions.math.distance

import jdk.incubator.vector.VectorSpecies

/**
 * Constants required for working with SIMD operations.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object SIMD {
    /** Flag indicating whether SIMD operations are enabled. */
    var SIMD_ENABLED = false

    /** The [VectorSpecies] used for [jdk.incubator.vector.FloatVector]. */
    @JvmStatic
    val FLOAT_VECTOR_SPECIES: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED

    /** The [VectorSpecies] used for [jdk.incubator.vector.DoubleVector]. */
    @JvmStatic
    val DOUBLE_VECTOR_SPECIES: VectorSpecies<Double> = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED

    /** The [VectorSpecies] used for [jdk.incubator.vector.LongVector]. */
    @JvmStatic
    val LONG_VECTOR_SPECIES: VectorSpecies<Long> = jdk.incubator.vector.LongVector.SPECIES_PREFERRED

    /** The [VectorSpecies] used for [ jdk.incubator.vector.IntVector]. */
    @JvmStatic
    val INT_VECTOR_SPECIES: VectorSpecies<Int> = jdk.incubator.vector.IntVector.SPECIES_PREFERRED
}