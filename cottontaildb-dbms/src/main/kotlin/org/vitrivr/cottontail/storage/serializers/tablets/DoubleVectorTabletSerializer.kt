package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [DoubleVectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorTabletSerializer(size: Int): AbstractTabletSerializer<DoubleVectorValue>(Types.DoubleVector(size)) {
    override fun writeToBuffer(value: DoubleVectorValue) = value.data.forEach {
        this.dataBuffer.putDouble(it)
    }
    override fun readFromBuffer(): DoubleVectorValue
        = DoubleVectorValue(DoubleArray(this.type.logicalSize) { this.dataBuffer.double } )
}