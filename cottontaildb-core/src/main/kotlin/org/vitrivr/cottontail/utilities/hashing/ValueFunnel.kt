package org.vitrivr.cottontail.utilities.hashing

import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import java.nio.charset.Charset

/**
 * This is a [Funnel] implementation used for [Value]s. It is typically used to generate hashes from [Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ValueFunnel: Funnel<Value?> {

    /**
     * Funnels the content of the given [Value] into the [PrimitiveSink].
     *
     * Makes sure, that different combinations of types yield different results,
     * by introducing type entries and/or length information, e.g., for [Value]s.
     *
     * @param from The [Tuple] to put into the [PrimitiveSink].
     * @param into The [PrimitiveSink]
     */
    override fun funnel(from: Value?, into: PrimitiveSink) {
        val value = from as? PublicValue
        if (value == null) {
            into.putBoolean(true)
            return
        }
        into.putBoolean(false)
        when (value) {
            is BooleanValue -> into.putBoolean(value.value)
            is ByteValue -> into.putByte(value.value)
            is ShortValue -> into.putShort(value.value)
            is IntValue -> into.putInt(value.value)
            is LongValue -> into.putLong(value.value)
            is FloatValue -> into.putFloat(value.value)
            is DoubleValue -> into.putDouble(value.value)
            is DateValue -> into.putLong(value.value)
            is Complex32Value -> {
                into.putFloat(value.data[0])
                into.putFloat(value.data[1])
            }
            is Complex64Value -> {
                into.putDouble(value.data[0])
                into.putDouble(value.data[1])
            }
            is StringValue -> into.putString(value.value, Charset.forName("UTF-8"))
            is UuidValue -> {
                into.putLong(value.value.leastSignificantBits)
                into.putLong(value.value.mostSignificantBits)
            }
            is ByteStringValue -> into.putBytes(value.value)
            is BooleanVectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putBoolean(it) }
            }
            is IntVectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putInt(it) }
            }
            is LongVectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putLong(it) }
            }
            is FloatVectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putFloat(it) }
            }
            is DoubleVectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putDouble(it) }
            }
            is Complex32VectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putFloat(it) }
            }
            is Complex64VectorValue -> {
                into.putInt(value.logicalSize)
                value.data.forEach { into.putDouble(it) }
            }
            null -> { /* No op. */ }
        }
    }
}