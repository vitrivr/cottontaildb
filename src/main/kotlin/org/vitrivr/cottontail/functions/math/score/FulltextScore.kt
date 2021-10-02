package org.vitrivr.cottontail.functions.math.score

import org.vitrivr.cottontail.functions.basics.Argument
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * This function is a mere placeholder used to express Lucene searches.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FulltextScore<T : VectorValue<*>>: Function.Stateless<StringValue> {
    override val signature: Signature.Closed<out StringValue>
        = Signature.Closed(Name.FunctionName("fulltext"), *arrayOf(Argument.Typed(Type.String)), Type.String)
    override val cost: Float = Float.MIN_VALUE
    override fun invoke(vararg arguments: Value?): StringValue {
        throw UnsupportedOperationException("LuceneScore cannot be executed directly.")
    }
}