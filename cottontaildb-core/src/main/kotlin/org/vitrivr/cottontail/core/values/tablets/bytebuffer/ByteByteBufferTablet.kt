package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [Value.Byte]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ByteByteBufferTablet(size: Int, direct: Boolean): Tablet.Byte, AbstractByteBufferTablet<ByteValue>(size, Types.Byte, direct) {
    override fun internalGet(index: Int): ByteValue = ByteValue(this.buffer.get(indexToPosition(index)))
    override fun internalSet(index: Int, value: ByteValue) { this.buffer.put(indexToPosition(index), value.value) }
}