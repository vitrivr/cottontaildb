package org.vitrivr.cottontail.dbms.index.hnsw

/**
 * Murmur3 is successor to Murmur2 fast non-crytographic hash algorithms.
 *
 * Murmur3 32 and 128 bit variants.
 * 32-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#94
 * 128-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
 *
 * This is a public domain code with no copyrights.
 * From homepage of MurmurHash (https://code.google.com/p/smhasher/),
 * "All MurmurHash versions are public domain software, and the author disclaims all copyright
 * to their code."
 */
object Murmur3 {
    // Constants for 32 bit variant
    private const val C1_32 = -0x3361d2af
    private const val C2_32 = 0x1b873593
    private const val R1_32 = 15
    private const val R2_32 = 13
    private const val M_32 = 5
    private const val N_32 = -0x19ab949c
    private const val DEFAULT_SEED = 104729
    /**
     * Murmur3 32-bit variant.
     *
     * @param data   - input byte array
     * @param offset - offset of data
     * @param length - length of array
     * @param seed   - seed. (default 0)
     * @return - hashcode
     */
    /**
     * Murmur3 32-bit variant.
     *
     * @param data - input byte array
     * @return - hashcode
     */
    @JvmOverloads
    @JvmStatic
    fun hash32(data: ByteArray, offset: Int = 0, length: Int = data.size, seed: Int = DEFAULT_SEED): Int {
        var hash = seed
        val nblocks = length shr 2

        // body
        for (i in 0 until nblocks) {
            val i_4 = i shl 2
            val k = (data[offset + i_4].toInt() and 0xff
                    or (data[offset + i_4 + 1].toInt() and 0xff shl 8)
                    or (data[offset + i_4 + 2].toInt() and 0xff shl 16)
                    or (data[offset + i_4 + 3].toInt() and 0xff shl 24))
            hash = mix32(k, hash)
        }

        // tail
        val idx = nblocks shl 2
        var k1 = 0
        when (length - idx) {
            3 -> {
                k1 = k1 xor (data[offset + idx + 2].toInt() shl 16)
                k1 = k1 xor (data[offset + idx + 1].toInt() shl 8)
                k1 = k1 xor data[offset + idx].toInt()

                // mix functions
                k1 *= C1_32
                k1 = Integer.rotateLeft(k1, R1_32)
                k1 *= C2_32
                hash = hash xor k1
            }

            2 -> {
                k1 = k1 xor (data[offset + idx + 1].toInt() shl 8)
                k1 = k1 xor data[offset + idx].toInt()
                k1 *= C1_32
                k1 = Integer.rotateLeft(k1, R1_32)
                k1 *= C2_32
                hash = hash xor k1
            }

            1 -> {
                k1 = k1 xor data[offset + idx].toInt()
                k1 *= C1_32
                k1 = Integer.rotateLeft(k1, R1_32)
                k1 *= C2_32
                hash = hash xor k1
            }
        }
        return fmix32(length, hash)
    }

    private fun mix32(kIn: Int, hashIn: Int): Int {
        var k = kIn
        var hash = hashIn
        k *= C1_32
        k = Integer.rotateLeft(k, R1_32)
        k *= C2_32
        hash = hash xor k
        return Integer.rotateLeft(hash, R2_32) * M_32 + N_32
    }

    private fun fmix32(length: Int, hashIn: Int): Int {
        var hash = hashIn
        hash = hash xor length
        hash = hash xor (hash ushr 16)
        hash *= -0x7a143595
        hash = hash xor (hash ushr 13)
        hash *= -0x3d4d51cb
        hash = hash xor (hash ushr 16)
        return hash
    }
}