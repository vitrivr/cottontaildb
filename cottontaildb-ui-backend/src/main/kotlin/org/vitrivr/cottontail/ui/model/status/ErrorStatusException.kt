package org.vitrivr.cottontail.ui.model.status

/**
 *
 */
data class ErrorStatusException(val code: Int, override val message: String) : Throwable(message) {
   fun toStatus() = ErrorStatus(this.code, this.message)
}