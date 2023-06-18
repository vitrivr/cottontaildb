package org.vitrivr.cottontail.core.values.tablets

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DateValue

/**
 * A [AbstractTablet] implementation for [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DateTablet(size: Int, direct: Boolean) : AbstractTablet<DateValue>(size, Types.Date, direct) {
    override fun internalGet(index: Int): DateValue = DateValue(this.buffer.getLong(indexToPosition(index)))
    override fun internalSet(index: Int, value: DateValue) { this.buffer.putLong(indexToPosition(index), value.value) }
}