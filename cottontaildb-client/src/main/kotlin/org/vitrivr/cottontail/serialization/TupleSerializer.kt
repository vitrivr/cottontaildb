package org.vitrivr.cottontail.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.PublicValue
import java.util.*

/** A [KSerializer] for a [List] of [PublicValue]s (= a tuple).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleSerializer(val columns: List<ColumnDef<*>>): KSerializer<Tuple> {

    /** The [TupleSerializer] returns a [List] of [PublicValue] types. */
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("TypedTuple") {
        for (c in this@TupleSerializer.columns) {
            element(c.name.simple, c.type.valueSerializer().descriptor)
        }
    }


    /** The [List] of [KSerializer]s for the different elements. */
    private val elementSerializers = columns.map {
        it.type.valueSerializer()
    }

    /**
     * Decodes a [List] of nullable [PublicValue]s
     *
     * @param decoder The [Decoder] instance.
     * @return The decoded [Tuple].
     */
    override fun deserialize(decoder: Decoder): Tuple {
        return decoder.decodeStructure(this@TupleSerializer.descriptor) {
            var prev: PublicValue? = null
            val list = LinkedList<PublicValue?>()
            for ((i, s) in this@TupleSerializer.elementSerializers.withIndex()) {
                prev = this.decodeNullableSerializableElement(this@TupleSerializer.descriptor, i, this@TupleSerializer.elementSerializers[i], prev)
                list.add(prev)
            }
            Tuple(this@TupleSerializer.columns, list)
        }
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
            for ((s, v) in this@TupleSerializer.elementSerializers.zip(value.values)) {
                encodeNullableSerializableElement(this@TupleSerializer.descriptor, index++, s, v)
            }
        }
    }
}