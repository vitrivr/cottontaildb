package org.vitrivr.cottontail.ui.model.session

import kotlinx.serialization.Serializable

/**
 * A [Session] as started by a user of the Thumper UI.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Session(val id: String, val connections: MutableList<Connection>) {
    companion object {
        const val USER_SESSION_KEY = "USER_SESSION"
    }
}