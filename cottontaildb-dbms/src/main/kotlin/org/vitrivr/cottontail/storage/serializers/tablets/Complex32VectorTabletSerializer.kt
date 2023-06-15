package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [Complex32VectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorTabletSerializer(size: Int): AbstractTabletSerializer<Complex32VectorValue>(Types.Complex32Vector(size)) {
    override fun writeToBuffer(value: Complex32VectorValue) = value.data.forEach {
        this.dataBuffer.putFloat(it)
    }
    override fun readFromBuffer(): Complex32VectorValue
        = Complex32VectorValue(FloatArray(2 * this.type.logicalSize) { this.dataBuffer.float } )
}