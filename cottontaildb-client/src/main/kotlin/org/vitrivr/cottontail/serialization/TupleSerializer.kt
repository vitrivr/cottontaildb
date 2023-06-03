package org.vitrivr.cottontail.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.PublicValue

/** A [KSerializer] for a [List] of [PublicValue]s (= a tuple).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleSerializer(val columns: Array<ColumnDef<*>>): KSerializer<Tuple> {

    /** The [TupleSerializer] generates a structure determined by the [List] of [ColumnDef]. */
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("Tuple[${columns.map { it.type }.joinToString(",")}]") {
        for (c in this@TupleSerializer.columns) {
            element(c.name.simple, c.type.valueSerializer().descriptor)
        }
    }

    /** The [List] of [KSerializer]s for the different elements. */
    private val elementSerializers = this.columns.map {
        it.type.valueSerializer()
    }

    /**
     * Decodes a [List] of nullable [PublicValue]s
     *
     * @param decoder The [Decoder] instance.
     * @return The decoded [Tuple].
     */
    override fun deserialize(decoder: Decoder): Tuple {
        val values = ArrayList<Value?>(this.columns.size)
        val dec = decoder.beginStructure(this.descriptor)
        while (true) {
            val index = dec.decodeElementIndex(this.descriptor)
            if (index == CompositeDecoder.DECODE_DONE) break
            val serializer = this.elementSerializers[index]
            values.add(dec.decodeNullableSerializableElement(this@TupleSerializer.descriptor, index, serializer))
        }
        dec.endStructure(this.descriptor)
        return StandaloneTuple(0L, this@TupleSerializer.columns, values.toTypedArray())
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
        for ((s, v) in this@TupleSerializer.elementSerializers.zip(value.values())) {
            enc.encodeNullableSerializableElement(this@TupleSerializer.descriptor, index++, s, v as? PublicValue)
        }
        enc.endStructure(this.descriptor)
    }
}