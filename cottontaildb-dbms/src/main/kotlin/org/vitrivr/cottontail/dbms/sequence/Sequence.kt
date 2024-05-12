package org.vitrivr.cottontail.dbms.sequence

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.sequences.Sequence

/**
 * Represents a [Sequence] in the Cottontail DB data model.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface Sequence: DBO {
    /** The [Catalogue] this [Sequence] belongs to. */
    override val parent: Schema

    /** The [Name.SequenceName] of this [Sequence]. */
    override val name: Name.SequenceName

    /**
     * Creates a new [SubTransaction] for the given [QueryContext] and parent [CatalogueTx].
     *
     * @param parent The parent [SchemaTx] object.
     * @return New [SequenceTx]
     */
    fun newTx(parent: SchemaTx): SequenceTx
}