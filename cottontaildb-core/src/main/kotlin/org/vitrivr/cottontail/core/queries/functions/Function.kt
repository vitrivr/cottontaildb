package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.nodes.CopyableNode
import org.vitrivr.cottontail.core.queries.nodes.Node
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.values.types.Value

/**
 * An invokable [Function] is a [Node] that can be used by Cottontail DB to calculate values results.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
interface Function<out R: Value>: NodeWithCost, CopyableNode {

    /** Signature of this [Function]. */
    val signature: Signature.Closed<out R>

    /** Flag indicating, that this [Function] can be executed. Defaults to true. */
    val executable: Boolean
        get() = true

    /**
     * Create a copy of this [Function]. Since by default, [Function]s are stateless,
     * this method will simply return a reference to this [Function].
     *
     * Can be overridden to enforce a different behaviour for stateful [Function] implementations.
     *
     * @return [Function]
     */
    override fun copy(): Function<R> = this

    /**
     * Invokes this [Function] with the given argument [Value]s and returns the output.
     *
     * @param arguments The argument [Value]s.
     * @return [R]
     */
    operator fun invoke(vararg arguments: Value?): R?

    /**
     * Obtains the [Digest] for this [Function].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = 127L * this.signature.hashCode()
}

