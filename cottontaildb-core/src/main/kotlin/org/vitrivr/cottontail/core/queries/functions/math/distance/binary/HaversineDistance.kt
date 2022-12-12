package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.sin
import kotlin.math.sqrt

/**
 * A [VectorDistance] implementation to calculate the Haversine distance between two 2D points.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.2.0
 */
sealed class HaversineDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName.create("haversine")

        const val RADIUS_EARTH = 6371000.0

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
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
                DoubleVector(Types.DoubleVector(2)).signature,
                FloatVector(Types.FloatVector(2)).signature,
                LongVector(Types.LongVector(2)).signature,
                IntVector(Types.IntVector(2)).signature
            )
        }
    }

    /** The [Cost] of applying this [HaversineDistance]. */
    override val cost: Cost
        get() = (Cost.FLOP * 27.0f + Cost.MEMORY_ACCESS * 22.0f) * this.d

    /**
     * Calculates the haversine distance between two points a, b.
     *
     * @param aLat The latitude of a.
     * @param aLon The longitude of a.
     * @param bLat The latitude of b.
     * @param bLon The longitude of b.
     */
    protected fun haversine(aLat: Double, aLon: Double, bLat: Double, bLon: Double): Double {
        val phi1 = StrictMath.toRadians(aLat)
        val phi2 = StrictMath.toRadians(bLat)
        val deltaPhi = StrictMath.toRadians(bLat - aLat)
        val deltaLambda = StrictMath.toRadians(bLon - aLon)
        val c = sin(deltaPhi / 2.0) * sin(deltaPhi / 2.0) + cos(phi1) * cos(phi2) * sin(deltaLambda / 2.0) * sin(deltaLambda / 2.0)
        val d = 2.0 * atan2(sqrt(c), sqrt(1 - c))
        return RADIUS_EARTH * d
    }

    /**
     * [HaversineDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): HaversineDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            return DoubleValue(haversine(query.data[0], query.data[1], probing.data[0], probing.data[1]))
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }

    /**
     * [HaversineDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): HaversineDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), probing.data[0].toDouble(), probing.data[1].toDouble()))
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
    }

    /**
     * [HaversineDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): HaversineDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), probing.data[0].toDouble(), probing.data[1].toDouble()))
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))
    }

    /**
     * [HaversineDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): HaversineDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), probing.data[0].toDouble(), probing.data[1].toDouble()))
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))
    }
}