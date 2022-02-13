package org.vitrivr.cottontail.dbms.schema

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.general.Tx

/**
 * A [Tx] that operates on a single [DefaultSchema].
 *
 * [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface SchemaTx : Tx {
    /**
     * Returns a list of [Entity] held by this [Schema].
     *
     * @return [List] of all [Entity].
     */
    fun listEntities(): List<Name.EntityName>

    /**
     * Returns an instance of [Entity] if such an instance exists. If the [Entity] has been loaded before,
     * that [Entity] is re-used. Otherwise, the [Entity] will be loaded from disk.
     *
     * @param name Name of the [Entity] to access.
     * @return [Entity]
     */
    fun entityForName(name: Name.EntityName): Entity

    /**
     * Creates a new [Entity] in this [DefaultSchema].
     *
     * @param name The name of the [Entity] that should be created.
     * @param columns List of [ColumnDef] that specify the columns to create.
     * @return Newly created [Entity] for use in context of this [Tx]
     */
    fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): Entity

    /**
     * Drops an [Entity] in this [DefaultSchema].
     *
     * @param name The name of the [Entity] that should be dropped.
     */
    fun dropEntity(name: Name.EntityName)
}