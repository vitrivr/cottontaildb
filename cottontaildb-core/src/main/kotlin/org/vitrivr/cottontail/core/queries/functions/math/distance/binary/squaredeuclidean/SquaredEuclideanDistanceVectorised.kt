package org.vitrivr.cottontail.core.queries.functions.math.distance.binary.squaredeuclidean

import jdk.incubator.vector.VectorOperators
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.VectorisedFunction
import org.vitrivr.cottontail.core.queries.functions.math.distance.SIMD
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistanceVectorised
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue

/**
 * A [SquaredEuclideanDistance] implementation that can use SIMD instructions.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class SquaredEuclideanDistanceVectorised<T : VectorValue<*>>(type: Types.Vector<T,*>): SquaredEuclideanDistance<T>(type), VectorisedFunction<DoubleValue> {
    override val name: Name.FunctionName = FUNCTION_NAME

    /** The loop-bound for this [EuclideanDistanceVectorised]. */
    protected abstract val loopBound: Int

    /** The step-size for this [EuclideanDistanceVectorised]. */
    protected abstract val stepSize: Int

    /** The [Cost] of applying this [EuclideanDistance]. */
    override val cost: Cost by lazy {
        ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 2.0f) * (this.vectorSize / SIMD.FLOAT_VECTOR_SPECIES.length()))
    }

    /**
     * [SquaredEuclideanDistanceVectorised] for a [FloatVectorValue]
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): SquaredEuclideanDistanceVectorised<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME

        /** The loop bound for this function. */
        override val loopBound = SIMD.FLOAT_VECTOR_SPECIES.loopBound(this.vectorSize)

        /** The step-size for this function. */
        override val stepSize = SIMD.FLOAT_VECTOR_SPECIES.length()

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as FloatVectorValue).data
            val query = (arguments[1] as FloatVectorValue).data

            /* Vectorised distance calculation. */
            var sum = 0.0f
            for (i in 0 until this.loopBound step this.stepSize) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(SIMD.FLOAT_VECTOR_SPECIES, probing, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(SIMD.FLOAT_VECTOR_SPECIES, query, i)
                val diff = vp.sub(vq)
                sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
            }

            /* Tail loop: Scalar version for remainder. */
            for (i in this.loopBound until this.vectorSize) {
                val diff: Float = query[i] - probing[i]
                sum = Math.fma(diff, diff, sum)
            }
            return DoubleValue(sum)
        }

        override fun invokeOrMaximum(left: VectorValue<*>, right: VectorValue<*>, maximum: DoubleValue): DoubleValue {
            val probing = (left as FloatVectorValue).data
            val query = (right as FloatVectorValue).data

            /* Vectorised distance calculation. */
            var sum = 0.0f
            for (i in 0 until this.loopBound step this.stepSize) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(SIMD.FLOAT_VECTOR_SPECIES, probing, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(SIMD.FLOAT_VECTOR_SPECIES, query, i)
                val diff = vp.sub(vq)
                sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
                if (sum >= maximum.value) break
            }

            /* Tail loop: Scalar version for remainder. */
            for (i in this.loopBound until this.vectorSize) {
                val diff: Float = query[i] - probing[i]
                sum = Math.fma(diff, diff, sum)
                if (sum >= maximum.value) break
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
    }

    /**
     * [SquaredEuclideanDistanceVectorised] for a [DoubleVectorValue]
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): SquaredEuclideanDistanceVectorised<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME

        /** The loop bound for this function. */
        override val loopBound = SIMD.DOUBLE_VECTOR_SPECIES.loopBound(this.vectorSize)

        /** The step-size for this function. */
        override val stepSize = SIMD.DOUBLE_VECTOR_SPECIES.length()

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as DoubleVectorValue).data
            val query = (arguments[1] as DoubleVectorValue).data

            /* Vectorised distance calculation. */
            var sum = 0.0
            for (i in 0 until this.loopBound step this.stepSize) {
                val vp = jdk.incubator.vector.DoubleVector.fromArray(SIMD.DOUBLE_VECTOR_SPECIES, probing, i)
                val vq = jdk.incubator.vector.DoubleVector.fromArray(SIMD.DOUBLE_VECTOR_SPECIES, query, i)
                val diff = vp.sub(vq)
                sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
            }

            /* Tail loop: Scalar version for remainder. */
            for (i in this.loopBound until this.vectorSize) {
                val diff: Double = query[i] - probing[i]
                sum = Math.fma(diff, diff, sum)
            }
            return DoubleValue(sum)
        }

        override fun invokeOrMaximum(left: VectorValue<*>, right: VectorValue<*>, maximum: DoubleValue): DoubleValue {
            val probing = (left as DoubleVectorValue).data
            val query = (right as DoubleVectorValue).data

            /* Vectorised distance calculation. */
            var sum = 0.0
            for (i in 0 until this.loopBound step this.stepSize) {
                val vp = jdk.incubator.vector.DoubleVector.fromArray(SIMD.DOUBLE_VECTOR_SPECIES, probing, i)
                val vq = jdk.incubator.vector.DoubleVector.fromArray(SIMD.DOUBLE_VECTOR_SPECIES, query, i)
                val diff = vp.sub(vq)
                sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
                if (sum >= maximum.value) break
            }

            /* Tail loop: Scalar version for remainder. */
            for (i in this.loopBound until this.vectorSize) {
                val diff: Double = query[i] - probing[i]
                sum = Math.fma(diff, diff, sum)
                if (sum >= maximum.value) break
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }
}