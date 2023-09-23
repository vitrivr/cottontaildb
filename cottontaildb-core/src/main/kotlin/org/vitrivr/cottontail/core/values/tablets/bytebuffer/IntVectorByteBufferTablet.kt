package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorByteBufferTablet(size: Int, logicalSize: Int, direct: Boolean): Tablet.IntVector, AbstractByteBufferTablet<IntVectorValue>(size, Types.IntVector(logicalSize), direct) {
    override fun internalGet(index: Int): IntVectorValue = IntVectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: IntVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putInt(it) }
        this.buffer.position(0)
    }
}