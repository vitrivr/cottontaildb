package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue

/**
 * A [AbstractTablet] implementation for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntTablet(size: Int, direct: Boolean): AbstractTablet<IntValue>(size, Types.Int, direct) {
    override fun internalGet(index: Int): IntValue = IntValue(this.buffer.getInt(indexToPosition(index)))
    override fun internalSet(index: Int, value: IntValue) { this.buffer.putInt(indexToPosition(index), value.value) }
}