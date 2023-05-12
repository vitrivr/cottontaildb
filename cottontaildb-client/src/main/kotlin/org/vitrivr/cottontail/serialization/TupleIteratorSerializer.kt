package org.vitrivr.cottontail.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.listSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.values.PublicValue

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleIteratorSerializer(private val elementSerializer: KSerializer<Tuple>): KSerializer<Iterator<Tuple>> {

    /** The [TupleSerializer] returns a [List] of [PublicValue] types. */
    override val descriptor: SerialDescriptor = listSerialDescriptor(
        listSerialDescriptor(
            PublicValue.serializer().descriptor
        )
    )

    /**
     * Decodes a [List] of nullable [PublicValue]s
     *
     * @param decoder The [Decoder] instance.
     * @return The decoded value.
     */
    override fun deserialize(decoder: Decoder): Iterator<Tuple> = object: Iterator<Tuple> {
        private val dec = decoder.beginStructure(this@TupleIteratorSerializer.descriptor)
        private var size = this.dec.decodeCollectionSize(this@TupleIteratorSerializer.descriptor)
        private var index = 0
        private var previous: Tuple? = null
        override fun hasNext(): Boolean = this.index < this.size
        override fun next(): Tuple {
            val tuple = this.dec.decodeSerializableElement(this@TupleIteratorSerializer.descriptor, this.index++, this@TupleIteratorSerializer.elementSerializer, this.previous)
            this.previous = tuple
            return tuple
        }
    }

    /**
     * Encodes a [Iterator] of [Tuple]s
     *
     * @param encoder The [Encoder] instance.
     * @param value The value to encode.
     */
    override fun serialize(encoder: Encoder, value: Iterator<Tuple>) {
        var i = 0
        val enc = encoder.beginStructure(this.descriptor)
        while (value.hasNext()) {
            enc.encodeSerializableElement(this.descriptor, i++, this.elementSerializer, value.next())
        }
        enc.endStructure(descriptor)
    }
}