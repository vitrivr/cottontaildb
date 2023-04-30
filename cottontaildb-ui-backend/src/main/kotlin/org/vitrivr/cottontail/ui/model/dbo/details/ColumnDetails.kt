package org.vitrivr.cottontail.ui.model.dbo.details

import org.vitrivr.cottontail.client.language.basics.Type

/**
 * A [ColumnDetails] returned by the Thumper API.
 *
 * @version 1.0.0
 */
data class ColumnDetails(val fqn: String, val type: Type, val length: Int, val nullable: Boolean, val autoIncrement: Boolean = false) {
    val simple: String = this.fqn.split('.').last()
}