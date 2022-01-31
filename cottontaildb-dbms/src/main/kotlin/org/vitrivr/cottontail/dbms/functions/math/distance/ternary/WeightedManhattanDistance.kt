package org.vitrivr.cottontail.dbms.functions.math.distance.ternary

import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.functions.math.WeightedVectorDistance
import org.vitrivr.cottontail.dbms.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.absoluteValue

/**
 * A weighted version of the Manhattan or L1 distance between two [VectorValue]s. The weight vector is an additional, third argument
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class WeightedManhattanDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): WeightedVectorDistance<T>(type) {

    /**
     * The [FunctionGenerator] for the [WeightedManhattanDistance].
     */
    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("manhattanw")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
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


    /** The cost of applying this [ManhattanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (2.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS)

    /**
     * [WeightedManhattanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): WeightedManhattanDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            val weight = arguments[2] as DoubleVectorValue

            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).absoluteValue * weight.data[i]
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }

    /**
     * [WeightedManhattanDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): WeightedManhattanDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            val weight = arguments[2] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).absoluteValue * weight.data[i]
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
    }

    /**
     * [WeightedManhattanDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): WeightedManhattanDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            val weight = arguments[2] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).absoluteValue * weight.data[i]
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))
    }

    /**
     * [WeightedManhattanDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): WeightedManhattanDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            val weight = arguments[2] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).absoluteValue * weight.data[i]
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))
    }
}