package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [DoubleValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleTabletSerializer: AbstractTabletSerializer<DoubleValue>(Types.Double) {
    override fun writeToBuffer(value: DoubleValue) {
        this.dataBuffer.putDouble(value.value)
    }
    override fun readFromBuffer(): DoubleValue {
        return DoubleValue(this.dataBuffer.double)
    }
}