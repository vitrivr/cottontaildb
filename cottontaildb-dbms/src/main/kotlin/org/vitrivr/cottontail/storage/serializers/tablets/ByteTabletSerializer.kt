package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.ShortValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ByteTabletSerializer: AbstractTabletSerializer<ByteValue>(Types.Byte) {
    override fun writeToBuffer(value: ByteValue) {
        this.dataBuffer.put(value.value)
    }
    override fun readFromBuffer(): ByteValue {
        return ByteValue(this.dataBuffer.get())
    }
}