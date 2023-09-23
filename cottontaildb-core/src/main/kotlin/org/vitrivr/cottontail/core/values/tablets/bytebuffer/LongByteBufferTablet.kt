package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [Value.Long]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongByteBufferTablet(size: Int, direct: Boolean): Tablet.Long, AbstractByteBufferTablet<LongValue>(size, Types.Long, direct) {
    override fun internalGet(index: Int): LongValue = LongValue(this.buffer.getLong(indexToPosition(index)))
    override fun internalSet(index: Int, value: LongValue) { this.buffer.putLong(indexToPosition(index), value.value) }
}