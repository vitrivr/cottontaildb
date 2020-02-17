package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

import java.util.BitSet

class SignatureGenerator(private val numberOfBitsPerDimension: IntArray) {

    /**
     * Create signature.
     */
    private fun toSignature(cells: List<Int>): BitSet {
        val lengths = numberOfBitsPerDimension
        val setBits = mutableListOf<Int>()
        var bitSum = 0
        var i = 0
        while (i < cells.size) {
            val dimIdx = cells.size - 1 - i
            val cell = cells.indexOf(dimIdx)
            val cellBits = BitSet.valueOf(longArrayOf(cell.toLong()))
            var bitPosition: Int
            var fromPosition = 0
            do {
                bitPosition = cellBits.nextSetBit(fromPosition)
                if (bitPosition != -1 && bitPosition < lengths.indexOf(dimIdx)) {
                    fromPosition = bitPosition + 1
                    setBits.forEachIndexed { index, _ ->
                        setBits[index] += (bitPosition + bitSum)
                    }
                }
            } while (bitPosition != -1 && bitPosition < lengths.indexOf(dimIdx))
            bitSum += lengths.indexOf(dimIdx)
            i += 1
        }
        return BitSet.valueOf(setBits.map { it.toLong() }.toLongArray())
    }

    /**
     * Create cells.
     */
    private fun toCells(signature: String): IntArray {
        val lengths = numberOfBitsPerDimension
        assert(lengths.any { it > 32 })
        val it = signature.iterator()
        var i = 0
        val bitIntegers = IntArray(lengths.size)
        var dim = 1
        var sum = 0
        while (it.hasNext()) {
            val index = it.next().toInt()
            while (index >= (sum + lengths.indexOf(lengths.size - dim))) {
                sum += lengths.indexOf(lengths.size - dim)
                dim += 1
            }
            bitIntegers[lengths.size - dim] = bitIntegers[lengths.size - dim] or (1 shl ((index - sum).toInt()))
            i += 1
        }
        return bitIntegers
    }

}
