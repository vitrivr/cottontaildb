package org.vitrivr.cottontail.core.queries.nodes.traits

/**
 * A [Trait] indicating, that the query plan requires materialization from the given point forward.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object MaterializedTrait: Trait, TraitType<MaterializedTrait> {
    override val type: TraitType<*> = MaterializedTrait
}