package ch.unibas.dmi.dbis.cottontail.knn.metrics

enum class Distance : DistanceFunction {
    /**
     * L1 or Manhattan distance between two vectors. Vectors must be of the same size!
     */
    L1 {
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0.0) b[i] = 0.0f-b[i]
                b[i] = b[i] * weights[i]
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0.0) b[i] = 0.0-b[i]
                b[i] = b[i] * weights[i]
            }
            return b.sum()
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0.0) b[i] = 0.0f-b[i]
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0.0) b[i] = 0.0-b[i]
            }
            return b.sum()
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return StrictMath.sqrt(b.sum().toDouble())
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return StrictMath.sqrt(b.sum())
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return StrictMath.sqrt(b.sum().toDouble())
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return StrictMath.sqrt(b.sum())
        }
    },

    /**
     * Squared L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return b.sum()
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return b.sum()
        }
    }
}