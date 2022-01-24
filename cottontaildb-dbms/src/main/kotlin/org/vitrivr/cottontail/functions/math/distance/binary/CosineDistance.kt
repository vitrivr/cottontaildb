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
import kotlin.math.sqrt

/**
 * A [VectorDistance] implementation to calculate the Cosine distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class CosineDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(Generator.FUNCTION_NAME, type) {
    /**
     * The [FunctionGenerator] for the [CosineDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("cosine")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector), Types.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Types.DoubleVector -> DoubleVector(type.logicalSize)
            is Types.FloatVector -> FloatVector(type.logicalSize)
            is Types.IntVector -> IntVector(type.logicalSize)
            is Types.LongVector -> LongVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** The cost of applying this [CosineDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (6.0f * Cost.COST_FLOP + 4.0f * Cost.COST_MEMORY_ACCESS) + 4.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS

    /**
     * [CosineDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : CosineDistance<DoubleVectorValue>(Types.DoubleVector(size)) {
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].pow(2)
                normv += probing.data[i].pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : CosineDistance<FloatVectorValue>(Types.FloatVector(size)) {
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as FloatVectorValue
            val query = this.arguments[1] as FloatVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].pow(2)
                normv += probing.data[i].pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : CosineDistance<LongVectorValue>(Types.LongVector(size)) {
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as LongVectorValue
            val query = this.arguments[1] as LongVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].toDouble().pow(2)
                normv += probing.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : CosineDistance<IntVectorValue>(Types.IntVector(size)) {
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as IntVectorValue
            val query = this.arguments[1] as IntVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].toDouble().pow(2)
                normv += probing.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }
}