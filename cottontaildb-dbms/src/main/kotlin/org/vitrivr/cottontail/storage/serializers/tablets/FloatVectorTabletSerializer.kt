package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [FloatVectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorTabletSerializer(size: Int): AbstractTabletSerializer<FloatVectorValue>(Types.FloatVector(size)) {
    override fun writeToBuffer(value: FloatVectorValue) = value.data.forEach {
        this.dataBuffer.putFloat(it)
    }
    override fun readFromBuffer(): FloatVectorValue
        = FloatVectorValue(FloatArray(this.type.logicalSize) { this.dataBuffer.float } )
}