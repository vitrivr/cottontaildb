package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue

/**
 * A [AbstractTablet] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ByteTablet(size: Int, direct: Boolean): AbstractTablet<ByteValue>(size, Types.Byte, direct) {
    override fun internalGet(index: Int): ByteValue = ByteValue(this.buffer.get(indexToPosition(index)))
    override fun internalSet(index: Int, value: ByteValue) { this.buffer.put(indexToPosition(index), value.value) }
}