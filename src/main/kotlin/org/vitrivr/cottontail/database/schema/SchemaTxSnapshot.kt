package org.vitrivr.cottontail.database.schema

import org.vitrivr.cottontail.database.entity.Entity

import org.vitrivr.cottontail.database.general.TxSnapshot

/**
 * These is a [TxSnapshot] for [SchemaTx]. It tracks all [Entity] that have been created or dropped during the [SchemaTx]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface SchemaTxSnapshot : TxSnapshot {
    /**
     * List of [Entity]es created during this [SchemaTxSnapshot].
     *
     * Such [Entity]es are only available to the [SchemaTx] that created them.
     */
    val created: MutableList<Entity>

    /**
     * List of [Entity]es dropped during this [SchemaTxSnapshot].
     */
    val dropped: MutableList<Entity>
}