package org.vitrivr.cottontail.functions.math.distance.basics

import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A special type of [VectorDistance.Binary] is the [MinkowskiDistance], e.g., the L1, L2 or Lp distance.
 * This is a decorator interface with no special function.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface MinkowskiDistance<T : VectorValue<*>> : VectorDistance.Binary<T> {
    val p: Int
}