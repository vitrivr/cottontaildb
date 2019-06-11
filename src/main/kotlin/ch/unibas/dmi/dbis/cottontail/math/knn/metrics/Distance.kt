package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

enum class Distance : DistanceFunction {
    /**
     * L1 or Manhattan distance between two vectors. Vectors must be of the same size!
     */
    L1 {
        override val operations: Int = 1

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += StrictMath.abs(b[i] - a[i])
            }
            return sum
        }
    },

    /**
     * L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2 {
        override val operations: Int = 2
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double = StrictMath.sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double = StrictMath.sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double = StrictMath.sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double = StrictMath.sqrt(L2SQUARED(a, b, weights))
        override fun invoke(a: FloatArray, b: FloatArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
        override fun invoke(a: DoubleArray, b: DoubleArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
        override fun invoke(a: LongArray, b: LongArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
        override fun invoke(a: IntArray, b: IntArray): Double = StrictMath.sqrt(L2SQUARED(a, b))
    },

    /**
     * Squared L2 or Euclidian distance between two vectors. Vectors must be of the same size!
     */
    L2SQUARED {
        override val operations: Int = 2

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i])
            }
            return sum
        }
    },

    /**
     * Chi Squared distance between two vectors. Vectors must be of the same size!
     */
    CHISQUARED {

        override val operations: Int = 3

        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 1e-6) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) > 0) {
                    sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return sum
        }
    }
}