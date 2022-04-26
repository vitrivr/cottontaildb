package org.vitrivr.cottontail.core.queries.nodes.traits

/**
 * A [Trait] indicating, that the query plan cannot be partitioned from this point forward.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object NotPartitionableTrait: Trait, TraitType<NotPartitionableTrait> {
    override val type: TraitType<*> = NotPartitionableTrait
}