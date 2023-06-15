package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [IntVectorValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorTabletSerializer(size: Int): AbstractTabletSerializer<IntVectorValue>(Types.IntVector(size)) {
    override fun writeToBuffer(value: IntVectorValue) = value.data.forEach {
        this.dataBuffer.putInt(it)
    }
    override fun readFromBuffer(): IntVectorValue
        = IntVectorValue(IntArray(this.type.logicalSize) { this.dataBuffer.int } )
}