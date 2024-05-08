package org.vitrivr.cottontail.utilities.formats

import java.io.Closeable
import java.io.InputStream
import java.nio.ByteBuffer

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FVecsReader(private val stream: InputStream): Closeable, Iterator<FloatArray> {

    /** */
    private val buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN)

    /**
     *
     */
    override fun hasNext(): Boolean = this.stream.available() > 0

    override fun next(): FloatArray {
        this.stream.read(this.buffer.array())
        val d = this.buffer.clear().int
        return FloatArray(d) {
            this.stream.read(this.buffer.array())
            this.buffer.clear().float
        }
    }
    override fun close() = this.stream.close()
}