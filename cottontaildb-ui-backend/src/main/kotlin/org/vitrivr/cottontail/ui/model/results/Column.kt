package org.vitrivr.cottontail.ui.model.results

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.Types

/**
 * Representation of a [Column] as exposed by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Column(val name: String, val type: Types<*>)