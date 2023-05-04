package org.vitrivr.cottontail.utilities.math

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types

/**
 * Utilities and constants used for nearest neighbor search.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object KnnUtilities {
    /**
     * Generates and returns a [ColumnDef] for a distance column.
     *
     * @param name The [Name.EntityName] to generate the [ColumnDef] for.
     * @return [ColumnDef]
     */
    fun distanceColumnDef(name: Name.EntityName? = null) = ColumnDef(name?.column("distance") ?: Name.ColumnName("distance"), Types.Double, false)
}