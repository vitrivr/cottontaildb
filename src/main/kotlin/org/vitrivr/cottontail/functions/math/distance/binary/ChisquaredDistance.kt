package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.AbstractFunctionGenerator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.FunctionGenerator
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.VectorDistance
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [VectorDistance] implementation to calculate the Chi^2 distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ChisquaredDistance<T : VectorValue<*>>: VectorDistance<T> {
    /**
     * The [FunctionGenerator] for the [ChisquaredDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        const val FUNCTION_NAME = "chisquared"

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, Type.Double, arity = 1)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** Name of this [ChisquaredDistance]. */
    override val name: String = Generator.FUNCTION_NAME

    /** The cost of applying this [ChisquaredDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (5.0f * Cost.COST_FLOP + 4.0f * Cost.COST_MEMORY_ACCESS)

    /**
     * [ChisquaredDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : ChisquaredDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as DoubleVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += ((this.query.data[i] - vector.data[i]).pow(2)) / (this.query.data[i] + vector.data[i])
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : ChisquaredDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as FloatVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += ((this.query.data[i] - vector.data[i]).pow(2)) / (this.query.data[i] + vector.data[i])
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : ChisquaredDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as LongVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += ((this.query.data[i] - vector.data[i]).toDouble().pow(2)) / (this.query.data[i] + vector.data[i])
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ChisquaredDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : ChisquaredDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as IntVectorValue
            var sum = 0.0
            for (i in this.query.data.indices) {
                sum += ((this.query.data[i] - vector.data[i]).toDouble().pow(2)) / (this.query.data[i] + vector.data[i])
            }
            return DoubleValue(sum)
        }
    }
}