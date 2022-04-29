package org.vitrivr.cottontail.core.queries.nodes

import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType

/**
 * A [Node] that imposes a certain trait on a query plan.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface NodeWithTrait: Node {
    /** Set of [Trait]s contained in this [Node]*/
    val traits: Map<TraitType<*>, Trait>

    /**
     * Checks, if this [NodeWithTrait] has a [Trait] of the given [TraitType].
     *
     * For this check, not the concrete implementation but the type of [Trait] is compared.
     */
    @Suppress("UNCHECKED_CAST")
    operator fun <T: Trait> get(type: TraitType<T>): T? = this.traits[type] as T?

    /**
     * Checks, if this [NodeWithTrait] has a [Trait] of the given [TraitType].
     *
     * @param type The [TraitType] to check for.
     * @return True on success, false otherwise.
     */
    fun hasTrait(type: TraitType<*>): Boolean = this.traits.contains(type)
}