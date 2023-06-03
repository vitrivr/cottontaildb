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
                into.putByte(0)
                into.putFloat(from.real.value)
                into.putFloat(from.imaginary.value)
            }
            is Complex64Value -> {
                into.putByte(1)
                into.putDouble(from.real.value)
                into.putDouble(from.imaginary.value)
            }
            is StringValue -> into.putString(from.value, Charset.forName("UTF-8"))
            is BooleanVectorValue -> {
                into.putByte(2)
                from.data.forEach { into.putBoolean(it) }
            }
            is IntVectorValue -> {
                into.putByte(3)
                from.data.forEach { into.putInt(it) }
            }
            is LongVectorValue -> {
                into.putByte(4)
                from.data.forEach { into.putLong(it) }
            }
            is FloatVectorValue -> {
                into.putByte(5)
                from.data.forEach { into.putFloat(it) }
            }
            is DoubleVectorValue -> {
                into.putByte(6)
                from.data.forEach { into.putDouble(it) }
            }
            is Complex32VectorValue -> {
                into.putByte(7)
                from.data.forEach { into.putFloat(it) }
            }
            is Complex64VectorValue -> {
                into.putByte(8)
                from.data.forEach { into.putDouble(it) }
            }
            null -> {
                into.putByte(-1)
                into.putByte(-1)
            }
        }
    }
}