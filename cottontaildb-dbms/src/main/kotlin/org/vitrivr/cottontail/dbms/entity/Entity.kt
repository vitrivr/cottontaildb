package org.vitrivr.cottontail.dbms.entity

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * Represents a single entity in the Cottontail DB data model. An [Entity] has name that must remain
 * unique within a [DefaultSchema]. The [Entity] contains one to many [Column]s holding the actual data.
 * Hence, it can be seen as a table containing tuples.
 *
 * @see Schema
 * @see Column
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface Entity : DBO {

    /** The [Name.EntityName] of this [Entity]. */
    override val name: Name.EntityName

    /** The [DefaultSchema] this [Entity] belongs to. */
    override val parent: Schema

    /**
     * Creates a new [SubTransaction] for the given [QueryContext] and parent [SchemaTx].
     *
     * @param parent The parent [SchemaTx] object.
     * @return New [EntityTx]
     */
    fun createOrResumeTx(parent: SchemaTx): EntityTx
}