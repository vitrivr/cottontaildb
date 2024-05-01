package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue

/**
 * A [AbstractTablet] implementation for [FloatVectorValue]s (single-precision).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorTablet(size: Int, logicalSize: Int, direct: Boolean): AbstractTablet<FloatVectorValue>(size, Types.FloatVector(logicalSize), direct) {
    override fun internalGet(index: Int): FloatVectorValue = FloatVectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: FloatVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putFloat(it) }
        this.buffer.position(0)
    }
}