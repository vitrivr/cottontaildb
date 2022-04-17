package org.vitrivr.cottontail.core.queries.nodes.traits

/**
 * A [Trait] describes a certain aspect of a query plan.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Trait {
    val type: TraitType<*>
}