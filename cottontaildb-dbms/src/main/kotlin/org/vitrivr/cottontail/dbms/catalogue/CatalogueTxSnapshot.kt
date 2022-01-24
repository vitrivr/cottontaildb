package org.vitrivr.cottontail.dbms.catalogue

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.general.TxSnapshot
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * These is a [TxSnapshot] for [CatalogueTx]. It tracks all [Schema]s that have been created or
 * dropped during the [CatalogueTx]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface CatalogueTxSnapshot : TxSnapshot {
    /** A map of all [Schema] structures available to the enclosing [CatalogueTx]. */
    val schemas: MutableMap<Name.SchemaName, Schema>
}