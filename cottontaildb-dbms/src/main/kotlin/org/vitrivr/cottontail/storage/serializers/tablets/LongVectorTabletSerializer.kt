package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [LongVectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorTabletSerializer(size: Int): AbstractTabletSerializer<LongVectorValue>(Types.LongVector(size)) {
    override fun writeToBuffer(value: LongVectorValue) = value.data.forEach {
        this.dataBuffer.putLong(it)
    }
    override fun readFromBuffer(): LongVectorValue
        = LongVectorValue(LongArray(this.type.logicalSize) { this.dataBuffer.long } )
}