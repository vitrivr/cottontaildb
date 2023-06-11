package org.vitrivr.cottontail.ui.model.status

import kotlinx.serialization.Serializable

/**
 * An [ErrorStatus] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class ErrorStatus(val code: Int, val description: String)