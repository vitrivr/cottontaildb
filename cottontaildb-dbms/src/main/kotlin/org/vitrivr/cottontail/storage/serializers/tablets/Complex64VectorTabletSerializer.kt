package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [Complex64VectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorTabletSerializer(size: Int): AbstractTabletSerializer<Complex64VectorValue>(Types.Complex64Vector(size)) {
    override fun writeToBuffer(value: Complex64VectorValue) = value.data.forEach {
        this.dataBuffer.putDouble(it)
    }
    override fun readFromBuffer(): Complex64VectorValue
        = Complex64VectorValue(DoubleArray(2 * this.type.logicalSize) { this.dataBuffer.double } )
}