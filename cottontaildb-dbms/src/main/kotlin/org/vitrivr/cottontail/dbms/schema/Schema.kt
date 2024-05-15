package org.vitrivr.cottontail.dbms.schema

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * Represents a [Schema] in the Cottontail DB data model. A [Schema] is a collection of [Entity]
 * objects that belong together (e.g., because they belong to the same application). Every [Schema]
 * can be seen as a dedicated database and different [Schema]s in Cottontail can reside in
 * different locations.
 *
 * @see Entity
 * @see Column
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
interface Schema : DBO {
    /** The [Catalogue] this [Schema] belongs to. */
    override val parent: Catalogue

    /** The [Name.SchemaName] of this [Schema]. */
    override val name: Name.SchemaName

    /**
     * Creates a new [SubTransaction] for the given [QueryContext] and parent [CatalogueTx].
     *
     * @param parent The parent [CatalogueTx] object.
     * @return New [SchemaTx]
     */
    fun newTx(parent: CatalogueTx): SchemaTx
}