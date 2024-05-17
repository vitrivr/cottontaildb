package org.vitrivr.cottontail.storage.ool.fixed

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import java.util.*

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueFixedLengthTestFile: AbstractFixedOOLFileTest<FloatVectorValue>() {
    override val type: Types<FloatVectorValue> = Types.FloatVector(SplittableRandom().nextInt(128, 2028))
}