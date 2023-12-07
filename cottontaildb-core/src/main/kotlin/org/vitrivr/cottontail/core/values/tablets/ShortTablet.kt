package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue

/**
 * A [AbstractTablet] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ShortTablet(size: Int, direct: Boolean): AbstractTablet<ShortValue>(size, Types.Short, direct) {
    override fun internalGet(index: Int): ShortValue = ShortValue(this.buffer.getShort(indexToPosition(index)))
    override fun internalSet(index: Int, value: ShortValue) { this.buffer.putShort(indexToPosition(index), value.value) }
}