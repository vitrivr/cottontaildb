package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorByteBufferTablet(size: Int, logicalSize: Int, direct: Boolean): Tablet.DoubleVector, AbstractByteBufferTablet<DoubleVectorValue>(size, Types.DoubleVector(logicalSize), direct) {
    override fun internalGet(index: Int): DoubleVectorValue = DoubleVectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: DoubleVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putDouble(it) }
        this.buffer.position(0)
    }
}