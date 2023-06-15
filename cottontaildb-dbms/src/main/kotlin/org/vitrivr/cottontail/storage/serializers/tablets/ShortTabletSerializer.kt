package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ShortTabletSerializer: AbstractTabletSerializer<ShortValue>(Types.Short) {
    override fun writeToBuffer(value: ShortValue) {
        this.dataBuffer.putShort(value.value)
    }
    override fun readFromBuffer(): ShortValue {
        return ShortValue(this.dataBuffer.short)
    }
}