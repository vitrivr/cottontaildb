package org.vitrivr.cottontail.serialization.kotlinx

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.PublicValue

/**
 * A [KSerializer] that serializes a [Tuple] into [List] of [PublicValue]s,
 * which is the most compact representation of a [Tuple].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleListSerializer(val columns: Array<ColumnDef<*>>): KSerializer<Tuple> {

    /** The [TupleSerializer] generates a structure determined by the [List] of [ColumnDef]. */
    override val descriptor: SerialDescriptor = ListSerializer(PublicValue.serializer()).descriptor

    /** The [List] of [KSerializer]s for the different elements. */
    private val elementSerializers = this.columns.map {
        it.type.serializer()
    }

    /**
     * Decodes a [List] of nullable [PublicValue]s
     *
     * @param decoder The [Decoder] instance.
     * @return The decoded [Tuple].
     */
    override fun deserialize(decoder: Decoder): Tuple {
        val values = Array<Value?>(this.columns.size) { null }
        val dec = decoder.beginStructure(this.descriptor)
        while (true) {
            val index = dec.decodeElementIndex(this.descriptor)
            if (index == CompositeDecoder.DECODE_DONE) break
            values[index] = dec.decodeNullableSerializableElement(this.descriptor, index, this.elementSerializers[index])
        }
        dec.endStructure(this.descriptor)
        return StandaloneTuple(0L, this.columns, values)
    }

    /**
     * Encodes a [List] of nullable [PublicValue]s
     *
     * @param encoder The [Encoder] instance.
     * @param value The [Tuple] to encode.
     */
    override fun serialize(encoder: Encoder, value: Tuple) {
        val enc = encoder.beginStructure(this.descriptor)
        var index = 0
        for ((s, v) in this@TupleListSerializer.elementSerializers.zip(value.values())) {
            enc.encodeNullableSerializableElement(this@TupleListSerializer.descriptor, index++, s, v as? PublicValue)
        }
        enc.endStructure(this.descriptor)
    }
}