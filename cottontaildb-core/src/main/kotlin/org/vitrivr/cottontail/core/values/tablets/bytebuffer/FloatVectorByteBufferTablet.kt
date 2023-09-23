package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorByteBufferTablet(size: Int, logicalSize: Int, direct: Boolean): Tablet.FloatVector, AbstractByteBufferTablet<FloatVectorValue>(size, Types.FloatVector(logicalSize), direct) {
    override fun internalGet(index: Int): FloatVectorValue = FloatVectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: FloatVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putFloat(it) }
        this.buffer.position(0)
    }
}