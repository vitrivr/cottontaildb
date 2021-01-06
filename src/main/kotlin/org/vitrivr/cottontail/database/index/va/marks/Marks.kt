package org.vitrivr.cottontail.database.index.va.marks

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.mapdb.serializer.SerializerDoubleArray

/**
 *
 */
inline class Marks(val marks: Array<DoubleArray>) {

    /**
     * This methods calculates the signature of the vector.
     * This method checks for every mark if the corresponding vector is beyond or not.
     * If so, mark before is the corresponding mark.
     *
     * Note that this can return -1, which means that the component is smaller than the smallest mark!
     * This can arise e.g. if marks are not generated from entire dataset, but just a sampled subset thereof!
     *
     * @param vector The vector.
     * @return An [IntArray] containing the signature of the vector.
     */
    fun getCells(vector: DoubleArray): IntArray = IntArray(vector.size) {
        val index = marks[it].indexOfFirst { i -> i > vector[it] }
        if (index == -1) { // all marks are less or equal than the vector component! last mark is what we're looking for!
            marks[it].size - 1
        } else {
            index - 1
        }
    }

    object MarksSerializer: Serializer<Marks> {
        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         *
         * @throws IOException in case of an I/O error
         */
        override fun serialize(out: DataOutput2, value: Marks) {
            out.packInt(value.marks.size)
            value.marks.forEach { dim ->
                out.packInt(dim.size)
                dim.forEach { marksInDim ->
                    out.writeDouble(marksInDim)
                }
            }
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
        override fun deserialize(input: DataInput2, available: Int) = Marks(Array(input.unpackInt()) {
            DoubleArray(input.unpackInt()) {
                input.readDouble()
            }
        })
    }
}