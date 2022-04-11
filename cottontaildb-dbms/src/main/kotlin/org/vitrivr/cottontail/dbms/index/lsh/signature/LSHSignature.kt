package org.vitrivr.cottontail.dbms.index.lsh.signature

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.index.lsh.LSHIndex
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

/**
 * A signature used by an [LSHIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@JvmInline
value class LSHSignature(private val buckets: IntArray): Comparable<LSHSignature> {
    /**
     * A [ComparableBinding] to serialize and deserialize [VAFSignature].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<LSHSignature> = LSHSignature(Snappy.uncompressIntArray(stream.readAllBytes()))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<LSHSignature>) {
            check(`object` is LSHSignature) { "LSHSignature.Binding can only be used to serialize LSHSignatures."}
            val compressed = Snappy.compress(`object`.buckets)
            output.write(compressed)
        }
    }

    /**
     * The [LSHSignature] is ordered in lexical order.
     */
    override fun compareTo(other: LSHSignature): Int {
        for ((i,b) in this.buckets.withIndex()) {
            if (i >= other.buckets.size) return Int.MIN_VALUE
            val comp = b.compareTo(other.buckets[i])
            if (comp != 0)  return comp
        }
        return 0
    }
}