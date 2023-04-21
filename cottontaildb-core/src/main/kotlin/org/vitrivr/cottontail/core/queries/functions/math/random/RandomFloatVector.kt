package org.vitrivr.cottontail.core.queries.functions.math.random

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * An [Function] that generates and returns a random value.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
object RandomFloatVector: Function<FloatVectorValue> {
    /** The [Name.FunctionName] backing this [RandomFloatVector]. */
    val FUNCTION_NAME = Name.FunctionName("rnd_floatvec")

    /** [Cost] of executing this function. TODO: This is probably very much off... */
    override val cost: Cost = Cost.FLOP

    /** This [RandomFloatVector]'s [Function] [Signature]. */
    override val signature: Signature.Closed<out FloatVectorValue>
        get() = Signature.Closed(FUNCTION_NAME, emptyArray<Argument.Typed<*>>(), Types.FloatVector(-1))

    /**
     * Generates and returns random value according to arguments.
     */
    override fun invoke(vararg arguments: Value?): FloatVectorValue = FloatVectorValueGenerator.random((arguments[0] as IntValue).value)

    /**
     * Generates a copy of this [RandomFloatVector].
     */
    override fun copy(): RandomFloatVector = RandomFloatVector
}