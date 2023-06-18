package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [AbstractTablet] implementation for [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleTablet(size: Int, direct: Boolean): AbstractTablet<DoubleValue>(size, Types.Double, direct) {
    override fun internalGet(index: Int): DoubleValue = DoubleValue(this.buffer.getDouble(indexToPosition(index)))
    override fun internalSet(index: Int, value: DoubleValue) { this.buffer.putDouble(indexToPosition(index), value.value) }
}