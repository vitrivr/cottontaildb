package org.vitrivr.cottontail.ui.model.system

/**
 * A [Lock] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Lock (val dbo: String, val mode: String, val ownerCount: Int, val owners: String)