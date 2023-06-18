package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue

/**
 * A [AbstractTablet] implementation for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongTablet(size: Int, direct: Boolean): AbstractTablet<LongValue>(size, Types.Long, direct) {
    override fun internalGet(index: Int): LongValue = LongValue(this.buffer.getLong(indexToPosition(index)))
    override fun internalSet(index: Int, value: LongValue) { this.buffer.putLong(indexToPosition(index), value.value) }
}