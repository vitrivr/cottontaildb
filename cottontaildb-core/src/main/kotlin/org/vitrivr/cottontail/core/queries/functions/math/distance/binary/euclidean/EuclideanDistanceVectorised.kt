package org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean

import jdk.incubator.vector.VectorOperators
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.VectorisedFunction
import org.vitrivr.cottontail.core.queries.functions.math.distance.SIMD.DOUBLE_VECTOR_SPECIES
import org.vitrivr.cottontail.core.queries.functions.math.distance.SIMD.FLOAT_VECTOR_SPECIES
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import kotlin.math.sqrt

/**
 * A [EuclideanDistance] implementation that can use SIMD instructions.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class EuclideanDistanceVectorised<T : VectorValue<*>>(type: Types.Vector<T,*>): EuclideanDistance<T>(type), VectorisedFunction<DoubleValue> {
    override val name: Name.FunctionName = FUNCTION_NAME

    /** The loop-bound for this [EuclideanDistanceVectorised]. */
    protected abstract val loopBound: Int

    /** The step-size for this [EuclideanDistanceVectorised]. */
    protected abstract val stepSize: Int

    /** The [Cost] of applying this [EuclideanDistance]. */
    override val cost: Cost by lazy {
        ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 2.0f) * (this.vectorSize / FLOAT_VECTOR_SPECIES.length())) + Cost.OP_SQRT
    }

    /**
     * [EuclideanDistanceVectorised] for a [FloatVectorValue]
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): EuclideanDistanceVectorised<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME

        /** The loop bound for this function. */
        override val loopBound = FLOAT_VECTOR_SPECIES.loopBound(this.vectorSize)

        /** The step-size for this function. */
        override val stepSize = FLOAT_VECTOR_SPECIES.length()

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as FloatVectorValue).data
            val query = (arguments[1] as FloatVectorValue).data

            /* Vectorised distance calculation. */
            var sum = 0.0f
            for (i in 0 until this.loopBound step this.stepSize) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(FLOAT_VECTOR_SPECIES, probing, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(FLOAT_VECTOR_SPECIES, query, i)
                val diff = vp.sub(vq)
                sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
            }

            /* Tail loop: Scalar version for remainder. */
            for (i in this.loopBound until this.vectorSize) {
                val diff: Float = query[i] - probing[i]
                sum = Math.fma(diff, diff, sum)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
    }

    /**
     * [EuclideanDistanceVectorised] for a [DoubleVectorValue]
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): EuclideanDistanceVectorised<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME

        /** The loop bound for this function. */
        override val loopBound = DOUBLE_VECTOR_SPECIES.loopBound(this.vectorSize)

        /** The step-size for this function. */
        override val stepSize = DOUBLE_VECTOR_SPECIES.length()

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as DoubleVectorValue).data
            val query = (arguments[1] as DoubleVectorValue).data

            /* Vectorised distance calculation. */
            var sum = 0.0
            for (i in 0 until this.loopBound step this.stepSize) {
                val vp = jdk.incubator.vector.DoubleVector.fromArray(DOUBLE_VECTOR_SPECIES, probing, i)
                val vq = jdk.incubator.vector.DoubleVector.fromArray(DOUBLE_VECTOR_SPECIES, query, i)
                val diff = vp.sub(vq)
                sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
            }

            /* Tail loop: Scalar version for remainder. */
            for (i in this.loopBound until this.vectorSize) {
                val diff: Double = query[i] - probing[i]
                sum = Math.fma(diff, diff, sum)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }
}