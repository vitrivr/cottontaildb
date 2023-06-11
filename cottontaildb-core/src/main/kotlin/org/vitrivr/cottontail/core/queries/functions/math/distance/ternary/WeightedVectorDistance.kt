package org.vitrivr.cottontail.core.queries.functions.math.distance.ternary

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A ternary, [Function] used for weighted, distance calculations between a query [VectorValue] and other vector values.
 *
 * These type of [Function] always accept three of the same [Types.Vector] (query argument, probing argument and weight)
 * and always return a [Types.Double].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class WeightedVectorDistance<R: NumericValue<*>, T : VectorValue<*>>(val type: Types.Vector<T,R>): Function<R> {
    /** The [Types.Vector] accepted by this [WeightedVectorDistance]. */
    abstract val name: Name.FunctionName

    /** The dimensionality of this [WeightedVectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /** Signature of a [WeightedVectorDistance] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<R>
        get() = Signature.Closed(name, arrayOf(this.type, this.type, this.type), this.type.elementType)

    /**
     * Creates a copy of this [WeightedVectorDistance].
     *
     * @return Copy of this [WeightedVectorDistance]
     */
    override fun copy(): WeightedVectorDistance<R, T> = copy(this.d)

    /**
     * Creates reshaped a copy of this [WeightedVectorDistance].
     *
     * @return Copy of this [WeightedVectorDistance]
     */
    abstract fun copy(d: Int): WeightedVectorDistance<R, T>
}