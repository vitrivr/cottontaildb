package org.vitrivr.cottontail.ui.model.system

import kotlinx.serialization.Serializable

/**
 * A [Lock] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Lock (val dbo: String, val mode: String, val ownerCount: Int, val owners: String)