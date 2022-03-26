package org.vitrivr.cottontail.core.queries.nodes.traits

/**
 * A [Trait] indicating, that the resultset of the query plan is deliberately limited.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class LimitTrait(val limit: Long): Trait {
    companion object: TraitType<LimitTrait>
    override val type: TraitType<*> = LimitTrait
}