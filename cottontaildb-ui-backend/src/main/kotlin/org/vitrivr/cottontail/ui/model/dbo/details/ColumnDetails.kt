package org.vitrivr.cottontail.ui.model.dbo.details

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types


/**
 * A [ColumnDetails] returned by the Thumper API.
 *
 * @version 1.0.0
 */
@Serializable
data class ColumnDetails(val name: Name.ColumnName, val type: Types<*>, val nullable: Boolean, val autoIncrement: Boolean = false) {
    val simple: String = this.name.simple
}