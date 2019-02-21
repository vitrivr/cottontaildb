package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

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

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0L) b[i] = 0L-b[i]
                sum += b[i] * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                b[i] = b[i] - a[i]
                if (b[i] < 0) b[i] = 0-b[i]
                sum += b[i] * weights[i]
            }
            return sum
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

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return StrictMath.sqrt(sum)
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                sum += (b[i] - a[i]) * (b[i] - a[i]) * weights[i]
            }
            return StrictMath.sqrt(sum)
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
    },


    /**
     * Chi Squared distance between two vectors. Vectors must be of the same size!
     */
    CHISQUARED {
        override fun invoke(a: FloatArray, b: FloatArray, weights: FloatArray): Double {
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) < 1e-6) {
                    b[i] = 0f
                }else{
                    b[i] = ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }

            }
            return b.sum().toDouble()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray, weights: DoubleArray): Double {
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) < 1e-6) {
                    b[i] = 0.0
                } else {
                    b[i] = ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
                }
            }
            return b.sum()
        }

        override fun invoke(a: LongArray, b: LongArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if((a[i] + b[i]) == 0L){
                    continue
                }
                sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray, weights: FloatArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if((a[i] + b[i]) == 0){
                    continue
                }
                sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i]) * weights[i]
            }
            return sum
        }

        override fun invoke(a: FloatArray, b: FloatArray): Double {
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) < 1e-6) {
                    b[i] = 0f
                } else {
                    b[i] = ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return b.sum().toDouble()
        }

        override fun invoke(a: DoubleArray, b: DoubleArray): Double {
            for (i in 0 until b.size) {
                if (Math.abs(a[i] + b[i]) < 1e-6) {
                    b[i] = 0.0
                } else {
                    b[i] = ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
                }
            }
            return b.sum()
        }

        override fun invoke(a: LongArray, b: LongArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if((a[i] + b[i]) == 0L){
                    continue
                }
                sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
            }
            return sum
        }

        override fun invoke(a: IntArray, b: IntArray): Double {
            var sum = 0.0
            for (i in 0 until b.size) {
                if((a[i] + b[i]) == 0){
                    continue
                }
                sum += ((b[i] - a[i]) * (b[i] - a[i])) / (b[i] + a[i])
            }
            return sum
        }
    }
}