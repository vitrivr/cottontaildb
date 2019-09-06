package ch.unibas.dmi.dbis.cottontail.database.index.va


class Quantizer(val vector: FloatArray, bits: UByte) {

    private val bounds: FloatArray
    private val bitLen: Int = 1 shl bits.toInt()

    init {
        bounds = quantizeVector(vector)
    }

    private fun quantizeVector(vector: FloatArray): FloatArray {
        val values = vector.filter { it.isFinite() }.sorted()
        val indexes = (1 until bitLen).map { (it * values.size) / bitLen }
        return indexes.map { values[it] }.toFloatArray()
    }

    fun quantize(value: Float): Int {
        val index = bounds.binarySearch(value)
        return if (index >= 0) index else -index - 1
    }

    fun getRange(index: Int): ClosedRange<Float>? {
        return when (index) {
            0 -> Float.NEGATIVE_INFINITY.rangeTo(bounds.first())
            in 1 until bitLen - 1 -> bounds[index - 1].rangeTo(bounds[index])
            bitLen - 1 -> bounds.last().rangeTo(Float.POSITIVE_INFINITY)
            else -> null
        }
    }

    fun quantize(vector: FloatArray): IntArray = vector.map { quantize(it) }.toIntArray()

}