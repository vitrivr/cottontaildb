package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [Value.Float]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatByteBufferTablet(size: Int, direct: Boolean): Tablet.Float, AbstractByteBufferTablet<FloatValue>(size, Types.Float, direct) {
    override fun internalGet(index: Int): FloatValue= FloatValue(this.buffer.getFloat(indexToPosition(index)))
    override fun internalSet(index: Int, value: FloatValue) { this.buffer.putFloat(indexToPosition(index), value.value) }
}