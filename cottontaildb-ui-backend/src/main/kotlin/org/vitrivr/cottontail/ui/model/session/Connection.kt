package org.vitrivr.cottontail.ui.model.session

import kotlinx.serialization.Serializable

/**
 * A [Connection] as used by the Thumper UI.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Connection(val host: String, val port: Int) {
    companion object {
        /**
         * Parses a [Connection] from a string.
         *
         * @param string The string to parse.
         * @return [Connection]
         */
        fun parse(string: String): Connection {
            val parts = string.split(":")
            return Connection(parts[0], parts[1].toInt())
        }
    }
}