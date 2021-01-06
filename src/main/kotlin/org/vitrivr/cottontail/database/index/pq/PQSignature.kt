package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * This class is used to represent an approximation of a vector in the DB
 * A signature contains for each subspace k the index of the representing
 * centroid entry of the k-th codebook
 * -> Such a signature is only meaningful in combination with K codebooks
 */
data class PQSignature(val tid: Long, val signature: IntArray) {
    companion object Serializer: org.mapdb.Serializer<PQSignature> {
        private val intSerializer = org.mapdb.Serializer.INT_ARRAY!!
        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         *
         * @throws IOException in case of an I/O error
         */
        override fun serialize(out: DataOutput2, value: PQSignature) {
            out.packLong(value.tid)
            intSerializer.serialize(out, value.signature)
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         * @throws IOException in case of an I/O error
         */
        override fun deserialize(input: DataInput2, available: Int) = PQSignature(input.unpackLong(), intSerializer.deserialize(input, available))
    }
}