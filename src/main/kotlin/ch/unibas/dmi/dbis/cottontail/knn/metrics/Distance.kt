package ch.unibas.dmi.dbis.cottontail.knn.metrics
enum class Distance : DistanceFunction {

    L1 {
        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var dist = 0.0
            for (i in a.indices) {
                dist += StrictMath.abs(b[i] - a[i])
            }
            return dist
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var dist = 0.0
            for (i in a.indices) {
                dist += StrictMath.abs(b[i] - a[i])
            }
            return dist
        }
    },
    L2 {
        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var dist = 0.0
            for (i in a.indices) {
                dist += StrictMath.pow((b[i] - a[i]).toDouble(), 2.0)
            }
            return Math.sqrt(dist)
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var dist = 0.0
            for (i in a.indices) {
                dist += StrictMath.pow((b[i] - a[i]), 2.0)
            }
            return Math.sqrt(dist)
        }
    }
}