package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntTabletSerializer: AbstractTabletSerializer<IntValue>(Types.Int) {
    override fun writeToBuffer(value: IntValue) {
        this.dataBuffer.putInt(value.value)
    }
    override fun readFromBuffer(): IntValue {
        return IntValue(this.dataBuffer.int)
    }
}