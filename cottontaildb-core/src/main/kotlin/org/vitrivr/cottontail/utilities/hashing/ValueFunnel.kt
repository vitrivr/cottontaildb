package org.vitrivr.cottontail.utilities.hashing

import com.google.common.hash.Funnel
import com.google.common.hash.PrimitiveSink
import org.vitrivr.cottontail.core.tuple.Tuple
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
        if (from == null) {
            into.putBoolean(true)
            return
        }
        into.putBoolean(false)
        when (from) {
            is BooleanValue -> into.putBoolean(from.value)
            is ByteValue -> into.putByte(from.value)
            is ShortValue -> into.putShort(from.value)
            is IntValue -> into.putInt(from.value)
            is LongValue -> into.putLong(from.value)
            is FloatValue -> into.putFloat(from.value)
            is DoubleValue -> into.putDouble(from.value)
            is DateValue -> into.putLong(from.value)
            is Complex32Value -> {
                into.putFloat(from.data[0])
                into.putFloat(from.data[1])
            }
            is Complex64Value -> {
                into.putDouble(from.data[0])
                into.putDouble(from.data[1])
            }
            is StringValue -> into.putString(from.value, Charset.forName("UTF-8"))
            is UuidValue -> {
                into.putLong(from.value.leastSignificantBits)
                into.putLong(from.value.mostSignificantBits)
            }
            is ByteStringValue -> into.putBytes(from.value)
            is BooleanVectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putBoolean(it) }
            }
            is IntVectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putInt(it) }
            }
            is LongVectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putLong(it) }
            }
            is ShortVectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putShort(it) }
            }
            is FloatVectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putFloat(it) }
            }
            is DoubleVectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putDouble(it) }
            }
            is Complex32VectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putFloat(it) }
            }
            is Complex64VectorValue -> {
                into.putInt(from.logicalSize)
                from.data.forEach { into.putDouble(it) }
            }
            null -> { /* No op. */ }
        }
    }
}