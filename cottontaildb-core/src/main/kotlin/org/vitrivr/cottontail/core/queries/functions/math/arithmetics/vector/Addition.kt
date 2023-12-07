package org.vitrivr.cottontail.core.queries.functions.math.arithmetics.vector

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue

/**
 * A [Function] to add two vector values.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Addition<T : VectorValue<*>>(val type: Types.Vector<T,*>): Function<T> {

    companion object : FunctionGenerator<VectorValue<*>> {
        private val FUNCTION_NAME = Name.FunctionName.create("add")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Addition<*> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            return when(val type = signature.arguments[0].type) {
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                is Types.LongVector -> LongVector(type)
                is Types.IntVector -> IntVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (Companion.signature != signature) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            return listOf(
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature,
                LongVector(Types.LongVector(1)).signature,
                IntVector(Types.IntVector(1)).signature
            )
        }
    }

    /** The [Cost] of executing this vector [Addition]. */
    override val cost
        get() = (Cost.FLOP + Cost.MEMORY_ACCESS * 2) * this.d

    /** The [Signature.Closed] of this vector [Addition]. */
    override val signature = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type), this.type)

    /** The dimensionality of this vector [Addition]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * Addition of two [IntVectorValue].
     */
    class IntVector(type: Types.IntVector): Addition<IntVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): IntVectorValue {
            val left = arguments[0] as IntVectorValue
            val right = arguments[1] as IntVectorValue
            return IntVectorValue(IntArray(this.d) { left.data[it] + right.data[it] })
        }
    }

    /**
     * Addition of two [LongVectorValue].
     */
    class LongVector(type: Types.LongVector): Addition<LongVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): LongVectorValue {
            val left = arguments[0] as LongVectorValue
            val right = arguments[1] as LongVectorValue
            return LongVectorValue(LongArray(this.d) { left.data[it] + right.data[it] })
        }
    }

    /**
     * Addition of two [FloatVectorValue].
     */
    class FloatVector(type: Types.FloatVector): Addition<FloatVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): FloatVectorValue {
            val left = arguments[0] as FloatVectorValue
            val right = arguments[1] as FloatVectorValue
            return FloatVectorValue(FloatArray(this.d) { left.data[it] + right.data[it] })
        }
    }

    /**
     * Addition of two [DoubleVectorValue].
     */
    class DoubleVector(type: Types.DoubleVector): Addition<DoubleVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleVectorValue {
            val left = arguments[0] as DoubleVectorValue
            val right = arguments[1] as DoubleVectorValue
            return DoubleVectorValue(DoubleArray(this.d) { left.data[it] + right.data[it] })
        }
    }
}