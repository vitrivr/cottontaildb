package org.vitrivr.cottontail.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.encodeStructure
import org.vitrivr.cottontail.core.*
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.*
import java.util.*

/** A [KSerializer] for a [List] of [PublicValue]s (= a tuple).
 *
 * Uses a simple format, that encode complex structures to text (i.e. needed for CSV exports).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleSimpleSerializer(val columns: Array<ColumnDef<*>>): KSerializer<Tuple> {
    /** The [TupleSerializer] returns a [List] of [PublicValue] types. */
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("SimpleTuple[${columns.map { it.type }.joinToString(",")}]") {
        for (c in this@TupleSimpleSerializer.columns) {
            element(c.name.simple, when(c.type) {
                Types.Boolean -> Boolean.serializer().descriptor
                Types.Byte -> Byte.serializer().descriptor
                Types.Double -> Double.serializer().descriptor
                Types.Float -> Float.serializer().descriptor
                Types.Int -> Int.serializer().descriptor
                Types.Long -> Long.serializer().descriptor
                Types.Short -> Short.serializer().descriptor
                else -> String.serializer().descriptor
            })
        }
    }

    /** The [List] of [KSerializer]s for the different elements. */
    private val types = this.columns.map { it.type }

    /**
     * Decodes a [List] of nullable [PublicValue]s
     *
     * @param decoder The [Encoder] instance.
     * @return The decoded [Tuple].
     */
    override fun deserialize(decoder: Decoder): Tuple {
        val list = ArrayList<PublicValue?>(this.columns.size)
        val dec = decoder.beginStructure(this.descriptor)
        while (true) {
            val index = dec.decodeElementIndex(this.descriptor)
            if (index == CompositeDecoder.DECODE_DONE) break
            val value = when (this.types[index]) {
                Types.Boolean -> BooleanValue(dec.decodeBooleanElement(this.descriptor, index))
                Types.Byte -> ByteValue(dec.decodeByteElement(this.descriptor, index))
                Types.Double -> DoubleValue(dec.decodeDoubleElement(this.descriptor, index))
                Types.Float -> FloatValue(dec.decodeFloatElement(this.descriptor, index))
                Types.Int -> IntValue(dec.decodeIntElement(this.descriptor, index))
                Types.Long -> LongValue(dec.decodeLongElement(this.descriptor, index))
                Types.Short -> ShortValue(dec.decodeShortElement(this.descriptor, index))
                Types.String -> StringValue(dec.decodeStringElement(this.descriptor, index))
                Types.Uuid -> UuidValue(UUID.fromString(dec.decodeStringElement(this.descriptor, index)))
                Types.ByteString -> ByteStringValue.fromBase64(dec.decodeStringElement(this.descriptor, index))
                Types.Date -> DateValue(dec.decodeLongElement(this.descriptor, index))
                Types.Complex32 -> parseComplex32Description(dec.decodeStringElement(this.descriptor, index))
                Types.Complex64 -> parseComplex64Description(dec.decodeStringElement(this.descriptor, index))
                is Types.BooleanVector -> parseBooleanVectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.DoubleVector -> parseDoubleVectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.FloatVector -> parseFloatVectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.IntVector -> parseIntVectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.LongVector -> parseLongVectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.Complex32Vector -> parseComplex32VectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.Complex64Vector -> parseComplex64VectorValue(dec.decodeStringElement(this.descriptor, index))
                is Types.ShortVector -> parseShortVectorValue(dec.decodeStringElement(this.descriptor, index))
            }
            list.add(value)
        }
        dec.endStructure(this.descriptor)
        return StandaloneTuple(0L, this@TupleSimpleSerializer.columns, list.toTypedArray())
    }

    /**
     * Encodes a [List] of nullable [PublicValue]s
     *
     * @param encoder The [Encoder] instance.
     * @param value The [Tuple] to encode.
     */
    override fun serialize(encoder: Encoder, value: Tuple) {
        encoder.encodeStructure(this.descriptor) {
            var index = 0
            for (v in value.values()) {
                when(val cast = v as? PublicValue) {
                    null -> {}
                    is BooleanValue -> encodeBooleanElement(this@TupleSimpleSerializer.descriptor, index++, cast.value)
                    is ByteValue -> encodeByteElement(this@TupleSimpleSerializer.descriptor, index++, cast.value)
                    is DoubleValue -> encodeDoubleElement(this@TupleSimpleSerializer.descriptor, index++, cast.value)
                    is FloatValue -> encodeFloatElement(this@TupleSimpleSerializer.descriptor, index++, cast.value)
                    is IntValue -> encodeIntElement(this@TupleSimpleSerializer.descriptor, index++, cast.value)
                    is LongValue -> encodeLongElement(this@TupleSimpleSerializer.descriptor, index++, cast.value)
                    is ShortValue -> encodeShortElement(this@TupleSimpleSerializer.descriptor, index++,cast.value)
                    else -> encodeStringElement(this@TupleSimpleSerializer.descriptor, index++, cast.toDescription())
                }
            }
        }
    }
}