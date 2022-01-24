package org.vitrivr.cottontail.functions.math.distance.basics

import org.vitrivr.cottontail.functions.basics.AbstractFunction
import org.vitrivr.cottontail.functions.basics.Argument
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A binary, [AbstractFunction] used for weighted distance calculations between a query [VectorValue] and other vector values.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class WeightedVectorDistance<T : VectorValue<*>>(val name: Name.FunctionName, val type: Types.Vector<T,*>)
    : AbstractFunction<DoubleValue>(Signature.Closed(name, arrayOf(Argument.Typed(type), Argument.Typed(type), Argument.Typed(type)), Types.Double)) {

    /** The dimensionality of this [VectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * Creates a copy of this [WeightedVectorDistance]. Can be used to create different shapes by choosing a new value for [d].
     *
     * @param d The new dimensionality of the [WeightedVectorDistance].
     * @return New [WeightedVectorDistance]
     */
    abstract fun copy(d: Int = this.d): WeightedVectorDistance<T>
}