package ch.unibas.dmi.dbis.cottontail.knn.metrics

enum class Distance : DistanceFunction {
    /**
     * L1 or Manhattan distance between two vectors. Vectors must be of the same size!
     */
    L1 {
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

        override fun invoke(a: LongArray, b: LongArray): Double {
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0L) b[i] = 0L-b[i]
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0) b[i] = 0-b[i]
            }
            return b.sum().toDouble()
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
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

        override fun invoke(a: LongArray, b: LongArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return StrictMath.sqrt(b.sum().toDouble())
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return StrictMath.sqrt(b.sum().toDouble())
        }
    },

    /**
     * Squared L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {
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

        override fun invoke(a: LongArray, b: LongArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            for (i in 0 until b.size) {
                b[i] = (b[i] - a[i]) * (b[i] - a[i])
            }
            return b.sum().toDouble()
        }
    }
}