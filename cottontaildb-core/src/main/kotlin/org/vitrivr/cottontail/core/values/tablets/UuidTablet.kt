package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue

/**
 * A [AbstractTablet] implementation for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class UuidTablet(size: Int, direct: Boolean): AbstractTablet<UuidValue>(size, Types.Uuid, direct) {
    override fun internalGet(index: Int): UuidValue = UuidValue(
        this.buffer.getLong(indexToPosition(index)),
        this.buffer.getLong(indexToPosition(index + Long.SIZE_BYTES))
    )
    override fun internalSet(index: Int, value: UuidValue) {
        this.buffer.putLong(indexToPosition(index), value.mostSignificantBits)
        this.buffer.putLong(indexToPosition(index + Long.SIZE_BYTES), value.leastSignificantBits)
    }
}