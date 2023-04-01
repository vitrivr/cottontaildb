package org.vitrivr.cottontail.core.values.types

import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import org.vitrivr.cottontail.core.values.DateValue
import java.nio.charset.Charset

/**
 * Represents a scalar value of any type, i.e. a value that consists only of a one entry. This is an
 * abstraction over the existing primitive array types provided by Kotlin. It allows for the advanced
 * type system implemented by Cottontail DB.
 *
 * @version 1.0.0
 * @author Ralph Gasser
 */
interface ScalarValue<T: Any> : Value {
    /** Actual value of this [Value]. */
    val value: T

}