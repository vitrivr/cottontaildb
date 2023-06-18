package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue

/**
 * A [AbstractTablet] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorTablet(size: Int, logicalSize: Int, direct: Boolean): AbstractTablet<DoubleVectorValue>(size, Types.DoubleVector(logicalSize), direct) {
    override fun internalGet(index: Int): DoubleVectorValue = DoubleVectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: DoubleVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putDouble(it) }
        this.buffer.position(0)
    }
}