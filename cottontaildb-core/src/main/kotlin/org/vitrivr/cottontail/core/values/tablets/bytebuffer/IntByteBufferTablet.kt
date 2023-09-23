package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [Value.Int]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntByteBufferTablet(size: Int, direct: Boolean): Tablet.Int, AbstractByteBufferTablet<IntValue>(size, Types.Int, direct) {
    override fun internalGet(index: Int): IntValue = IntValue(this.buffer.getInt(indexToPosition(index)))
    override fun internalSet(index: Int, value: IntValue) { this.buffer.putInt(indexToPosition(index), value.value) }
}