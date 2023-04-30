package org.vitrivr.cottontail.ui.model.dbo.details

/**
 * [IndexDetails] returned by the Thumper API.
 *
 * @version 1.0.0
 */
data class IndexDetails(val fqn: String, val type: String) {
    val simple: String = this.fqn.split('.').last()
}