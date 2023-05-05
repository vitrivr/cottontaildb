package org.vitrivr.cottontail.ui.model.dbo.details

import kotlinx.serialization.Serializable

/**
 * [EntityDetails] returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class EntityDetails (val fqn: String, val size: Long, val columns: List<ColumnDetails> = emptyList(), val indexes: List<IndexDetails> = emptyList()) {
    val simple: String = this.fqn.split('.').last()
}