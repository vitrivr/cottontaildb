package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.functions.*
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.functions.math.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.pow

/**
 * A [VectorDistance] implementation to calculate the Chi^2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class ChisquaredDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(Generator.FUNCTION_NAME, type){
    /**
     * The [FunctionGenerator] for the [ChisquaredDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("chisquared")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector), Types.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Types.DoubleVector -> DoubleVector(type.logicalSize)
            is Types.FloatVector -> FloatVector(type.logicalSize)
            is Types.IntVector -> IntVector(type.logicalSize)
            is Types.LongVector -> LongVector(type.logicalSize)
            else ->  throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** The cost of applying this [ChisquaredDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (5.0f * Cost.COST_FLOP + 4.0f * Cost.COST_MEMORY_ACCESS)

    /**
     * [ChisquaredDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : ChisquaredDistance<DoubleVectorValue>(Types.DoubleVector(size)) {
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : ChisquaredDistance<FloatVectorValue>(Types.FloatVector(size)) {
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as FloatVectorValue
            val query = this.arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : ChisquaredDistance<LongVectorValue>( Types.LongVector(size)) {
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as LongVectorValue
            val query = this.arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).toDouble().pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : ChisquaredDistance<IntVectorValue>(Types.IntVector(size)) {
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(): DoubleValue {
            val probing =  this.arguments[0] as IntVectorValue
            val query = this.arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += ((query.data[i] - probing.data[i]).toDouble().pow(2)) / (query.data[i] + probing.data[i])
            }
            return DoubleValue(sum)
        }
    }
}