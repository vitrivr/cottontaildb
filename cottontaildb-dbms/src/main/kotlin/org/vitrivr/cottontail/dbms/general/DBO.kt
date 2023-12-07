package org.vitrivr.cottontail.dbms.general

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.sequence.SequenceTx

/**
 * A database object [DBO] in Cottontail DB (e.g., a schema, entity etc.). [DBO]s are identified by
 * a [Name] and usually part of a [DBO] hierarchy. Furthermore, they can be used to create [Tx]
 * objects that act on the [DBO].
 *
 * @version 3.0.0
 * @author Ralph Gasser
 */
interface DBO {
    /** The [Name] of this [DBO]. */
    val name: Name

    /** The [Catalogue] this [DBO] belongs to. */
    val catalogue: Catalogue

    /** The parent DBO (if such exists). */
    val parent: DBO?

    /** The [DBOVersion] of this [DBO]. */
    val version: DBOVersion
        get() = DBOVersion.current()

    /**
     * Creates a new [Tx] for the given [QueryContext].
     *
     * @param context [QueryContext] to create [Tx] for.
     */
    fun newTx(context: QueryContext): Tx
}