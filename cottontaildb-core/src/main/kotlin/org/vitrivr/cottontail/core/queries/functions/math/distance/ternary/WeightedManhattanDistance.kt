package org.vitrivr.cottontail.core.queries.functions.math.distance.ternary

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.NumericValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.absoluteValue

/**
 * A weighted version of the Manhattan or L1 distance between two [VectorValue]s. The weight vector is an additional, third argument
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class WeightedManhattanDistance<R: NumericValue<*>, T : VectorValue<*>>(type: Types.Vector<T,R>): WeightedVectorDistance<R, T>(type) {

    /**
     * The [FunctionGenerator] for the [WeightedManhattanDistance].
     */
    companion object: FunctionGenerator<NumericValue<*>> {
        val FUNCTION_NAME = Name.FunctionName("manhattanw")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): WeightedVectorDistance<*, *> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (!signature.arguments.all { it.type == signature.arguments[0].type }) { /* Only if all arguments have the same type, there is an actual match. */
                throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
            return when(val type = signature.arguments[0].type) {
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (Companion.signature != signature) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            return listOf(
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature
            )
        }
    }


    /** The [Cost] of applying this [WeightedManhattanDistance]. */
    override val cost: Cost
        get() = ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 3.0f) * this.d) + Cost.MEMORY_ACCESS

    /**
     * [WeightedManhattanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,DoubleValue>): WeightedManhattanDistance<DoubleValue, DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            val weight = arguments[2] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum = Math.fma((query.data[i] - probing.data[i]).absoluteValue, weight.data[i], sum)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }

    /**
     * [WeightedManhattanDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,FloatValue>): WeightedManhattanDistance<FloatValue, FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): FloatValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            val weight = arguments[2] as FloatVectorValue
            var sum = 0.0f
            for (i in query.data.indices) {
                sum = Math.fma((query.data[i] - probing.data[i]).absoluteValue, weight.data[i], sum)
            }
            return FloatValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
    }
}