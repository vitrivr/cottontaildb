package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value

/**
 * A [AbstractTablet] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64Tablet(size: Int, direct: Boolean) : AbstractTablet<Complex64Value>(size, Types.Complex64, direct) {
    override fun internalGet(index: Int): Complex64Value {
        val position = indexToPosition(index)
        return Complex64Value(this.buffer.getDouble(position), this.buffer.getDouble(position + Double.SIZE_BYTES))
    }
    override fun internalSet(index: Int, value: Complex64Value) {
        val position = indexToPosition(index)
        this.buffer.putDouble(position, value.data[0])
        this.buffer.putDouble(position + Double.SIZE_BYTES, value.data[1])
    }
}