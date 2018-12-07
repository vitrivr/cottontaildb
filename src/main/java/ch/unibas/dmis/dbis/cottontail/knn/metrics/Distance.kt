package ch.unibas.dmis.dbis.cottontail.knn.metrics

enum class Distance : DistanceFunction {
    L2 {
        override fun invoke(a: FloatArray, b: FloatArray): Float {
            var dist = 0.0
            for (i in a.indices) {
                dist += Math.pow((b[i] - a[i]).toDouble(), 2.0)
            }
            return Math.sqrt(dist).toFloat()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var dist = 0.0
            for (i in a.indices) {
                dist += Math.pow((b[i] - a[i]).toDouble(), 2.0)
            }
            return Math.sqrt(dist)
        }
    }
}