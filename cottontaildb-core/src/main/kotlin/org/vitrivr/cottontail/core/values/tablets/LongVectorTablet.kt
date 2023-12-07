package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue

/**
 * A [AbstractTablet] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorTablet(size: Int, logicalSize: Int, direct: Boolean): AbstractTablet<LongVectorValue>(size, Types.LongVector(logicalSize), direct) {
    override fun internalGet(index: Int): LongVectorValue = LongVectorValue(this.buffer.slice(this.indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: LongVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putLong(it) }
        this.buffer.position(0)
    }
}