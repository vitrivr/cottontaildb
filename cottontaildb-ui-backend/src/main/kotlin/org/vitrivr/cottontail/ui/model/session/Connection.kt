package org.vitrivr.cottontail.ui.model.session

import kotlinx.serialization.Serializable

/**
 * A [Connection] as used by the Cottontail DB UI.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Connection(val host: String, val port: Int)