package org.vitrivr.cottontail.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.encodeStructure
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.toDescription
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.*

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleSimpleSerializer(val columns: List<ColumnDef<*>>): KSerializer<Tuple> {
    /** The [TupleSerializer] returns a [List] of [PublicValue] types. */
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("TupleDescription") {
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

    override fun deserialize(decoder: Decoder): Tuple {
        TODO("Not yet implemented")
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
            for (v in value.values) {
                when(v) {
                    null -> {}
                    is BooleanValue -> encodeBooleanElement(this@TupleSimpleSerializer.descriptor, index++, v.value)
                    is ByteValue -> encodeByteElement(this@TupleSimpleSerializer.descriptor, index++, v.value)
                    is DoubleValue -> encodeDoubleElement(this@TupleSimpleSerializer.descriptor, index++, v.value)
                    is FloatValue -> encodeFloatElement(this@TupleSimpleSerializer.descriptor, index++, v.value)
                    is IntValue -> encodeIntElement(this@TupleSimpleSerializer.descriptor, index++, v.value)
                    is LongValue -> encodeLongElement(this@TupleSimpleSerializer.descriptor, index++, v.value)
                    is ShortValue -> encodeShortElement(this@TupleSimpleSerializer.descriptor, index++,v.value)
                    else -> encodeStringElement(this@TupleSimpleSerializer.descriptor, index++, v.toDescription())
                }
            }
        }
    }
}