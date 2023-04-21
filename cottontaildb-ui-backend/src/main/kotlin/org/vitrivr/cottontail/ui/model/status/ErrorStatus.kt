package org.vitrivr.cottontail.ui.model.status

/**
 * An [ErrorStatus] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class ErrorStatus(val code: Int, val description: String)