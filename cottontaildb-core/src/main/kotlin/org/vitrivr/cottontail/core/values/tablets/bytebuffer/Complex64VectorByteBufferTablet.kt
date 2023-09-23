package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * An [AbstractByteBufferTablet] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorByteBufferTablet (size: Int, logicalSize: Int, direct: Boolean): Tablet.Complex64Vector, AbstractByteBufferTablet<Complex64VectorValue>(size, Types.Complex64Vector(logicalSize), direct) {
    override fun internalGet(index: Int): Complex64VectorValue = Complex64VectorValue(this.buffer.slice(indexToPosition(index), this.type.physicalSize))
    override fun internalSet(index: Int, value: Complex64VectorValue) {
        this.buffer.position(indexToPosition(index))
        value.data.forEach { this.buffer.putDouble(it) }
        this.buffer.position(0)
    }
}