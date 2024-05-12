package org.vitrivr.cottontail.dbms.schema

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.sequence.Sequence

/**
 * A [SubTransaction] that operates on a single [DefaultSchema].
 *
 * [SubTransaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface SchemaTx : SubTransaction {
    /** The parent [CatalogueTx] this [SchemaTx] belongs to. */
    val parent: CatalogueTx

    /** The [QueryContext] this [SchemaTx] belongs to. Typically determined by parent [CatalogueTx]. */
    val context: QueryContext
        get() = this.parent.context

    /** The [Transaction] this [EntityTx] belongs to. Typically determined by parent [CatalogueTx]. */
    override val transaction: Transaction
        get() = this.context.txn

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
     * @param columns [List] of [Name.ColumnName] to [ColumnMetadata] [Pair]s that specify the columns to create in order.
     * @return Newly created [Entity] for use in context of this [SubTransaction]
     */
    fun createEntity(name: Name.EntityName, columns: List<Pair<Name.ColumnName, ColumnMetadata>>): Entity

    /**
     * Drops an [Entity] in the [Schema] underlying this [SchemaTx].
     *
     * @param name The name of the [Entity] that should be dropped.
     */
    fun dropEntity(name: Name.EntityName)

    /**
     * Creates a new [Sequence] in this [Schema].
     *
     * @param name The name of the [Sequence] that should be created.
     * @return Newly created [Sequence] for use in context of this [SubTransaction]
     */
    fun createSequence(name: Name.SequenceName): Sequence

    /**
     * Drops an [Sequence] in this [Schema].
     *
     * @param name The name of the [Name.SequenceName] that should be dropped.
     */
    fun dropSequence(name: Name.SequenceName)
}