package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.AbstractFunctionGenerator
import org.vitrivr.cottontail.functions.basics.Argument
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
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
sealed class HaversineDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(Generator.FUNCTION_NAME, type) {

    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("haversine")

        const val RADIUS_EARTH = 6371000.0

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector), Types.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when {
            dst.arguments[0].type is Types.DoubleVector && dst.arguments[0].type.logicalSize == 2 -> DoubleVector()
            dst.arguments[0].type is Types.FloatVector && dst.arguments[0].type.logicalSize == 2 -> FloatVector()
            dst.arguments[0].type is Types.LongVector && dst.arguments[0].type.logicalSize == 2 -> LongVector()
            dst.arguments[0].type is Types.IntVector && dst.arguments[0].type.logicalSize == 2 -> IntVector()
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** The cost of applying this [HaversineDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * 2.0f * Cost.COST_MEMORY_ACCESS + 27.0f * Cost.COST_FLOP + 20.0f * Cost.COST_MEMORY_ACCESS

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
        return Generator.RADIUS_EARTH * d
    }

    /**
     * [HaversineDistance] for a [DoubleVectorValue].
     */
    class DoubleVector: HaversineDistance<DoubleVectorValue>( Types.DoubleVector(2)) {
        override fun copy(d: Int) = DoubleVector()
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            return DoubleValue(haversine(query.data[0], query.data[1], probing.data[0], probing.data[1]))
        }
    }

    /**
     * [HaversineDistance] for a [FloatVectorValue].
     */
    class FloatVector: HaversineDistance<FloatVectorValue>(Types.FloatVector(2)) {
        override fun copy(d: Int) = FloatVector()
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as FloatVectorValue
            val query = this.arguments[1] as FloatVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), probing.data[0].toDouble(), probing.data[1].toDouble()))
        }
    }

    /**
     * [HaversineDistance] for a [LongVectorValue].
     */
    class LongVector: HaversineDistance<LongVectorValue>(Types.LongVector(2)) {
        override fun copy(d: Int) = LongVector()
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as LongVectorValue
            val query = this.arguments[1] as LongVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), probing.data[0].toDouble(), probing.data[1].toDouble()))
        }
    }

    /**
     * [HaversineDistance] for a [IntVectorValue].
     */
    class IntVector: HaversineDistance<IntVectorValue>(Types.IntVector(2)) {
        override fun copy(d: Int) = IntVector()
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as IntVectorValue
            val query = this.arguments[1] as IntVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), probing.data[0].toDouble(), probing.data[1].toDouble()))
        }
    }
}