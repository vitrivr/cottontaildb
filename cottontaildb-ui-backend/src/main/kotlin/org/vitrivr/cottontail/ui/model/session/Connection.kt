package org.vitrivr.cottontail.ui.model.session

/**
 * A [Connection] as used by the Cottontail DB UI.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Connection(val host: String, val port: Int)