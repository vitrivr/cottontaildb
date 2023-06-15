package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongTabletSerializer: AbstractTabletSerializer<LongValue>(Types.Long) {
    override fun writeToBuffer(value: LongValue) {
        this.dataBuffer.putLong(value.value)
    }
    override fun readFromBuffer(): LongValue {
        return LongValue(this.dataBuffer.long)
    }
}