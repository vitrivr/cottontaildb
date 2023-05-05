package org.vitrivr.cottontail.ui.model.dbo.details

import kotlinx.serialization.Serializable

/**
 * [IndexDetails] returned by the Thumper API.
 *
 * @version 1.0.0
 */
@Serializable
data class IndexDetails(val fqn: String, val type: String) {
    val simple: String = this.fqn.split('.').last()
}