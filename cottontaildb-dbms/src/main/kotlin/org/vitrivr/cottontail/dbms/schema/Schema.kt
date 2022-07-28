package org.vitrivr.cottontail.dbms.schema

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.entity.Entity
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
 * @version 3.0.0
 */
interface Schema : DBO {
    /** The [Catalogue] this [Schema] belongs to. */
    override val parent: Catalogue

    /** The [Name.SchemaName] of this [Schema]. */
    override val name: Name.SchemaName

    /**
     * Creates and returns a new [SchemaTx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [SchemaTx] for.
     * @return New [SchemaTx]
     */
    override fun newTx(context: QueryContext): SchemaTx
}