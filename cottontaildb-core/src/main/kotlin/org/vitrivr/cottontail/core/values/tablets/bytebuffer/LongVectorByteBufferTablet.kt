package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorByteBufferTablet(size: Int, logicalSize: Int, direct: Boolean): Tablet.LongVector, AbstractByteBufferTablet<LongVectorValue>(size, Types.LongVector(logicalSize), direct) {
    override fun internalGet(index: Int): LongVectorValue = LongVectorValue(this.buffer.slice(this.indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: LongVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.forEach { this.buffer.putLong(it.value) }
        this.buffer.position(0)
    }
}