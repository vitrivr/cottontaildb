package org.vitrivr.cottontail.database.catalogue

import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.schema.Schema

/**
 * These is a [TxSnapshot] for [CatalogueTx]. It tracks all [Schema]s that have been created or dropped during the [CatalogueTx]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CatalogueTxSnapshot : TxSnapshot {
    /**
     * List of [Schema]es created during this [CatalogueTx].
     *
     * Such [Schema]es are only available to the [CatalogueTx] that created them.
     */
    val created: MutableList<Schema>

    /**
     * List of [Schema]es dropped during this [CatalogueTx].
     */
    val dropped: MutableList<Schema>
}