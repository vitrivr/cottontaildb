package org.vitrivr.cottontail.core.queries.functions.math.score

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * This [Function] is a mere placeholder used to express fulltext searches. It cannot be executed directly.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object FulltextScore: Function<DoubleValue> {
    private val name = Name.FunctionName.create("fulltext")
    override val signature: Signature.Closed<out DoubleValue> = Signature.Closed(name, arrayOf(Argument.Typed(Types.String), Argument.Typed(Types.String)), Types.Double)
    override val executable: Boolean = false
    override val cost: Cost = Cost.INVALID
    override fun invoke(vararg arguments: Value?): DoubleValue = throw UnsupportedOperationException("Function $signature cannot be executed directly.")
    override fun copy(): Function<DoubleValue> = this
}