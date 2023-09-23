package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [Value.Short]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ShortByteBufferTablet(size: Int, direct: Boolean): Tablet.Short, AbstractByteBufferTablet<ShortValue>(size, Types.Short, direct) {
    override fun internalGet(index: Int): ShortValue = ShortValue(this.buffer.getShort(indexToPosition(index)))
    override fun internalSet(index: Int, value: ShortValue) { this.buffer.putShort(indexToPosition(index), value.value) }
}