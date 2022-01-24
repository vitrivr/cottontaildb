package org.vitrivr.cottontail.functions.math.distance.basics

import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A special type of [VectorDistance] is the [MinkowskiDistance], e.g., the L1, L2 or Lp distance.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class MinkowskiDistance<T : VectorValue<*>>(name: Name.FunctionName, type: Types.Vector<T,*>, val p: Int): VectorDistance<T>(name, type)