package org.vitrivr.cottontail.dbms.queries.operators

import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode

/**
 * A collection of utility functions that can be used when manipulating [OperatorNode] trees.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object OperatorNodeUtilities {

    /**
     * Appends every [OperatorNode.Logical] in the [chain] to [appendTo] if it matches the given [predicate].
     *
     * @param appendTo The [OperatorNode.Logical] to append chains to.
     * @param chain The [OperatorNode.Logical] tree to traverse and append elements from.
     * @param predicate The [predicate] to match every [OperatorNode.Logical] in the chain against.
     * @return Root of the new [OperatorNode.Logical]
     */
    inline fun chainIf(appendTo: OperatorNode.Logical, chain: OperatorNode.Logical, predicate: (OperatorNode.Logical) -> Boolean): OperatorNode.Logical {
        var ret = appendTo
        var next: OperatorNode.Logical? = chain
        while (next != null) {
            if (predicate(next)) {
                ret = next.copyWithNewInput(ret)
            }
            next = next.output
        }
        return ret
    }

    /**
     * Appends every [OperatorNode.Physical] in the [chain] to [appendTo] if it matches the given [predicate].
     *
     * @param appendTo The [OperatorNode.Physical] to append chains to.
     * @param chain The [OperatorNode.Physical] tree to traverse and append elements from.
     * @param predicate The [predicate] to match every [OperatorNode.Physical] in the chain against.
     * @return Root of the new [OperatorNode.Physical]
     */
    inline fun chainIf(appendTo: OperatorNode.Physical, chain: OperatorNode.Physical, predicate: (OperatorNode.Physical) -> Boolean): OperatorNode.Physical {
        var ret = appendTo
        var next: OperatorNode.Physical? = chain
        while (next != null) {
            if (predicate(next)) {
                ret = next.copyWithNewInput(ret)
            }
            next = next.output
        }
        return ret
    }
}