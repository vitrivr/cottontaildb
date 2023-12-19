package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.HalfVectorValue
import org.vitrivr.cottontail.utilities.math.Half

/**
 * A [AbstractTablet] implementation for [FloatVectorValue]s (half-precision).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HalfVectorTablet(size: Int, logicalSize: Int, direct: Boolean): AbstractTablet<HalfVectorValue>(size, Types.HalfVector(logicalSize), direct) {
    override fun internalGet(index: Int): HalfVectorValue {
        val buffer = this.buffer.slice(indexToPosition(index), this.type.physicalSize)
        return HalfVectorValue(FloatArray(this.type.logicalSize) { Half(buffer.getShort().toUShort()).toFloat() })
    }
    override fun internalSet(index: Int, value: HalfVectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach {
            this.buffer.putShort(Half(it).v.toShort())
        }
        this.buffer.position(0)
    }
}