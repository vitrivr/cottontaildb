package org.vitrivr.cottontail.functions.math.distance.ternary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.WeightedVectorDistance
import org.vitrivr.cottontail.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.functions.math.distance.ternary.WeightedManhattanDistance.Generator.FUNCTION_NAME
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.absoluteValue

/**
 * A weighted version of the Manhattan or L1 distance between two [VectorValue]s. The weight vector is an additional, third argument
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class WeightedManhattanDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): WeightedVectorDistance<T>(FUNCTION_NAME, type) {

    /**
     * The [FunctionGenerator] for the [WeightedManhattanDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("manhattanw")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector, Argument.Vector), Types.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Types.DoubleVector -> DoubleVector(type.logicalSize)
            is Types.FloatVector -> FloatVector(type.logicalSize)
            is Types.IntVector -> IntVector(type.logicalSize)
            is Types.LongVector -> LongVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }


    /** The cost of applying this [ManhattanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (2.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS)

    /**
     * [WeightedManhattanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : WeightedManhattanDistance<DoubleVectorValue>(Types.DoubleVector(size)) {
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            val weight = this.arguments[2] as DoubleVectorValue

            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]) * weight.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [WeightedManhattanDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : WeightedManhattanDistance<FloatVectorValue>(Types.FloatVector(size)) {
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as FloatVectorValue
            val query = this.arguments[1] as FloatVectorValue
            val weight = this.arguments[2] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]) * weight.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [WeightedManhattanDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : WeightedManhattanDistance<LongVectorValue>(Types.LongVector(size)) {
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as LongVectorValue
            val query = this.arguments[1] as LongVectorValue
            val weight = this.arguments[3] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]) * weight.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [WeightedManhattanDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : WeightedManhattanDistance<IntVectorValue>(Types.IntVector(size)) {
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as IntVectorValue
            val query = this.arguments[1] as IntVectorValue
            val weight = this.arguments[3] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]) * weight.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }
}