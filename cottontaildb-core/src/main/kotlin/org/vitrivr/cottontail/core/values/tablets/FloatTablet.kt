package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatValue

/**
 * A [AbstractTablet] implementation for [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatTablet(size: Int, direct: Boolean): AbstractTablet<FloatValue>(size, Types.Float, direct) {
    override fun internalGet(index: Int): FloatValue = FloatValue(this.buffer.getFloat(indexToPosition(index)))
    override fun internalSet(index: Int, value: FloatValue) { this.buffer.putFloat(indexToPosition(index), value.value) }
}