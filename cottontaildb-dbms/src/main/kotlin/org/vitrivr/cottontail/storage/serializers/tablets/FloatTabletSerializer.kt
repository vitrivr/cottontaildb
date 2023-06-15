package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [DoubleValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatTabletSerializer: AbstractTabletSerializer<FloatValue>(Types.Float) {
    override fun writeToBuffer(value: FloatValue) {
        this.dataBuffer.putFloat(value.value)
    }
    override fun readFromBuffer(): FloatValue {
        return FloatValue(this.dataBuffer.float)
    }
}