package org.vitrivr.cottontail.core.values.tablets.bytebuffer

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.tablets.Tablet

/**
 * A [AbstractByteBufferTablet] implementation for [Value.Date]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DateByteBufferTablet(size: Int, direct: Boolean): Tablet.Date, AbstractByteBufferTablet<DateValue>(size, Types.Date, direct) {
    override fun internalGet(index: Int): DateValue= DateValue(this.buffer.getLong(indexToPosition(index)))
    override fun internalSet(index: Int, value: DateValue) { this.buffer.putLong(indexToPosition(index), value.value) }
}