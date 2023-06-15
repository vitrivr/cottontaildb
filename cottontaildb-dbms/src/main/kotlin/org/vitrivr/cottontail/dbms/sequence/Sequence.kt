package org.vitrivr.cottontail.dbms.sequence

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import kotlin.sequences.Sequence

/**
 * Represents a [Sequence] in the Cottontail DB data model.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Sequence: DBO {
    /** The [Catalogue] this [Sequence] belongs to. */
    override val parent: Schema

    /** The [Name.SequenceName] of this [Sequence]. */
    override val name: Name.SequenceName

    /**
     * Creates and returns a new [SequenceTx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [SequenceTx] for.
     * @return New [SequenceTx]
     */
    override fun newTx(context: QueryContext): SequenceTx
}