package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [DoubleValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32TabletSerializer: AbstractTabletSerializer<Complex32Value>(Types.Complex32) {
    override fun writeToBuffer(value: Complex32Value) {
        this.dataBuffer.putFloat(value.data[0])
        this.dataBuffer.putFloat(value.data[1])
    }
    override fun readFromBuffer(): Complex32Value {
        return Complex32Value(floatArrayOf(this.dataBuffer.float, this.dataBuffer.float))
    }
}