package org.vitrivr.cottontail.storage.serializers.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.Tablet

/**
 * A [TabletSerializer] for [Tablet] that hold [DoubleValue]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DateTabletSerializer: AbstractTabletSerializer<DateValue>(Types.Date) {
    override fun writeToBuffer(value: DateValue) {
        this.dataBuffer.putLong(value.value)
    }
    override fun readFromBuffer(): DateValue {
        return DateValue (this.dataBuffer.long)
    }
}