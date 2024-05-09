package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A special type of [VectorDistance] is the [MinkowskiDistance], e.g., the L1, L2 or Lp distance.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class MinkowskiDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(type) {
    abstract val p: Int
}