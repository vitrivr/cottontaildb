package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [VectorDistance.Binary] implementation to calculate the Cosine distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class HammingDistance<T : VectorValue<*>>: VectorDistance.Binary<T> {
    /**
     * The [FunctionGenerator] for the [HammingDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("hamming")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector), Type.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Type.DoubleVector -> DoubleVector(type.logicalSize)
            is Type.FloatVector -> FloatVector(type.logicalSize)
            is Type.IntVector -> IntVector(type.logicalSize)
            is Type.LongVector -> LongVector(type.logicalSize)
            is Type.BooleanVector -> BooleanVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** The cost of applying this [HammingDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS)

    /** Name of this [HammingDistance]. */
    override val name: Name.FunctionName = Generator.FUNCTION_NAME

    /**
     * [HammingDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : HammingDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as DoubleVectorValue
        }
    }

    /**
     * [HammingDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : HammingDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int): VectorDistance.Binary<FloatVectorValue> = FloatVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as FloatVectorValue
        }
    }

    /**
     * [HammingDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : HammingDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int): VectorDistance.Binary<LongVectorValue> = LongVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as LongVectorValue
        }
    }

    /**
     * [HammingDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : HammingDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as IntVectorValue
        }
    }

    /**
     * [HammingDistance] for a [IntVectorValue].
     */
    class BooleanVector(size: Int) : HammingDistance<BooleanVectorValue>() {
        override val type = Type.BooleanVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = BooleanVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as BooleanVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != probing.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as BooleanVectorValue
        }
    }
}