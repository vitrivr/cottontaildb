package org.vitrivr.cottontail.storage.ool.random

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.storage.ool.fixed.AbstractFixedOOLFileTest

/**
 * A [AbstractFixedOOLFileTest] for [StringValue]s.

 * @author Ralph Gasser
 * @version 1.0.0
 */
class StringValueVariableOOLFileTest: AbstractVariableOOLFileTest<StringValue>() {
    override val type: Types<StringValue> = Types.String
}