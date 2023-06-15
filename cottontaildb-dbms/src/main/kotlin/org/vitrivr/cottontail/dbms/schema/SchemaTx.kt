package org.vitrivr.cottontail.dbms.schema

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.general.Tx
import org.vitrivr.cottontail.dbms.sequence.Sequence

/**
 * A [Tx] that operates on a single [DefaultSchema].
 *
 * [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface SchemaTx : Tx {
    /**
     * Returns a list of [Name.EntityName] held by this [Schema].
     *
     * @return [List] of all [Name.EntityName].
     */
    fun listEntities(): List<Name.EntityName>

    /**
     * Returns a list of [Name.SequenceName] held by this [Schema].
     *
     * @return [List] of all [Name.SequenceName].
     */
    fun listSequence(): List<Name.SequenceName>

    /**
     * Returns an instance of [Entity] if such an instance exists.
     *
     * @param name [Name.EntityName] of the [Entity] to access.
     * @return [Entity]
     */
    fun entityForName(name: Name.EntityName): Entity

    /**
     * Returns an instance of [Sequence] if such an instance exists.
     *
     * @param name [Name.SequenceName] of the [Sequence] to access.
     * @return [Sequence]
     */
    fun sequenceForName(name: Name.SequenceName): Sequence

    /**
     * Creates a new [Entity] in this [DefaultSchema].
     *
     * @param name The name of the [Entity] that should be created.
     * @param columns List of [ColumnDef] that specify the columns to create.
     * @return Newly created [Entity] for use in context of this [Tx]
     */
    fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): Entity

    /**
     * Drops an [Entity] in the [Schema] underlying this [SchemaTx].
     *
     * @param name The name of the [Entity] that should be dropped.
     */
    fun dropEntity(name: Name.EntityName)

    /**
     * Truncates an [Entity] in the [Schema] underlying this [SchemaTx].
     *
     * @param name The name of the [Entity] that should be truncated.
     */
    fun truncateEntity(name: Name.EntityName)

    /**
     * Creates a new [Sequence] in this [Schema].
     *
     * @param name The name of the [Sequence] that should be created.
     * @return Newly created [Sequence] for use in context of this [Tx]
     */
    fun createSequence(name: Name.SequenceName): Sequence

    /**
     * Drops an [Sequence] in this [Schema].
     *
     * @param name The name of the [Name.SequenceName] that should be dropped.
     */
    fun dropSequence(name: Name.SequenceName)
}