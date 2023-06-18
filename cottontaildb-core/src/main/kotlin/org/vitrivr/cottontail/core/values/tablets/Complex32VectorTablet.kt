package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue

/**
 * An [AbstractTablet] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorTablet(size: Int, logicalSize: Int, direct: Boolean): AbstractTablet<Complex32VectorValue>(size, Types.Complex32Vector(logicalSize), direct) {
    override fun internalGet(index: Int): Complex32VectorValue = Complex32VectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: Complex32VectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putFloat(it) }
        this.buffer.position(0)
    }
}