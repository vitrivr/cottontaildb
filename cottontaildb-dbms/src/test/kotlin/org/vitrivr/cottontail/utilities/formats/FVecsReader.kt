package org.vitrivr.cottontail.utilities.formats

import java.io.Closeable
import java.io.InputStream
import java.nio.ByteBuffer

/**
 * A [FVecsReader] reads a stream of float vectors from a FVECS binary file (e.g., used for the SIFT dataset).
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FVecsReader(private val stream: InputStream): Closeable, Iterator<FloatArray> {

    /** Internal buffer used to read 4 bytes. */
    private val buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN)

    /**
     * Checks if this [FVecsReader] has more vectors to read.
     *
     * @return True, if vectors are available, false otherwise.
     */
    override fun hasNext(): Boolean = this.stream.available() > 0

    /**
     * Reads the next [FloatArray] from the [InputStream].
     *
     * @return [FloatArray]
     */
    override fun next(): FloatArray {
        this.stream.read(this.buffer.array())
        val d = this.buffer.clear().int
        return FloatArray(d) {
            this.stream.read(this.buffer.array())
            this.buffer.clear().float
        }
    }

    /**
     * Closes this [FVecsReader].
     */
    override fun close() = this.stream.close()
}