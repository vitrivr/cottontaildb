package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.MinkowskiDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [SquaredEuclideanDistance] implementation to calculate the squared Euclidean or L2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class SquaredEuclideanDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): MinkowskiDistance<T>(Generator.FUNCTION_NAME, type, 2) {

    /**
     * The [FunctionGenerator] for the [SquaredEuclideanDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("squaredeuclidean")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector), Types.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Types.Complex64Vector -> Complex64Vector(type.logicalSize)
            is Types.Complex32Vector -> Complex32Vector(type.logicalSize)
            is Types.DoubleVector -> DoubleVector(type.logicalSize)
            is Types.FloatVector -> FloatVector(type.logicalSize)
            is Types.LongVector -> LongVector(type.logicalSize)
            is Types.IntVector -> IntVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** The cost of applying this [SquaredEuclideanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS

    /**
     * [SquaredEuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(size: Int) : SquaredEuclideanDistance<Complex64VectorValue>(Types.Complex64Vector(size)) {
        override fun copy(d: Int) = Complex64Vector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as Complex64VectorValue
            val query = this.arguments[1] as Complex64VectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(size: Int) : SquaredEuclideanDistance<Complex32VectorValue>(Types.Complex32Vector(size)) {
        override fun copy(d: Int) = Complex32Vector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as Complex32VectorValue
            val query = this.arguments[1] as Complex32VectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : SquaredEuclideanDistance<DoubleVectorValue>(Types.DoubleVector(size)) {
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : SquaredEuclideanDistance<FloatVectorValue>(Types.FloatVector(size)) {
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as FloatVectorValue
            val query = this.arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : SquaredEuclideanDistance<LongVectorValue>(Types.LongVector(size)) {
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as LongVectorValue
            val query = this.arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : SquaredEuclideanDistance<IntVectorValue>(Types.IntVector(size)) {
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as IntVectorValue
            val query = this.arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
    }
}