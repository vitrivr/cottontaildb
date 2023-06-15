package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [Complex64Value]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64TabletSerializer: AbstractTabletSerializer<Complex64Value>(Types.Complex64) {
    override fun writeToBuffer(value: Complex64Value) {
        this.dataBuffer.putDouble(value.data[0])
        this.dataBuffer.putDouble(value.data[1])
    }
    override fun readFromBuffer(): Complex64Value {
        return Complex64Value(doubleArrayOf(this.dataBuffer.double, this.dataBuffer.double))
    }
}