package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value

/**
 * A [AbstractTablet] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32Tablet(size: Int, direct: Boolean) : AbstractTablet<Complex32Value>(size, Types.Complex32, direct) {
    override fun internalGet(index: Int): Complex32Value {
        val position = indexToPosition(index)
        return Complex32Value(this.buffer.getFloat(position), this.buffer.getFloat(position + Float.SIZE_BYTES))
    }
    override fun internalSet(index: Int, value: Complex32Value) {
        val position = indexToPosition(index)
        this.buffer.putFloat(position, value.data[0])
        this.buffer.putFloat(position + Float.SIZE_BYTES, value.data[1])
    }
}