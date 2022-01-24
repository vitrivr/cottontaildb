package org.vitrivr.cottontail.dbms.schema

import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.general.TxSnapshot
import org.vitrivr.cottontail.core.database.Name

/**
 * These is a [TxSnapshot] for [SchemaTx]. It tracks all [Entity] that have been created or dropped during the [SchemaTx]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface SchemaTxSnapshot : TxSnapshot {
    /** A map of all [Entity] structures available to the enclosing [SchemaTx]. */
    val entities: MutableMap<Name.EntityName, Entity>
}