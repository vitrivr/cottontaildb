package org.vitrivr.cottontail.core.functions.math

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A special type of [VectorDistance] is the [MinkowskiDistance], e.g., the L1, L2 or Lp distance.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class MinkowskiDistance<T : VectorValue<*>>(name: Name.FunctionName, type: Types.Vector<T,*>, val p: Int): VectorDistance<T>(name, type)