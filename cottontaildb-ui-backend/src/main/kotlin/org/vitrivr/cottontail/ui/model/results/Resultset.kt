package org.vitrivr.cottontail.ui.model.results

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.values.PublicValue

/**
 * A generic [Resultset] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Resultset(val columns: List<Column>, val values: List<Array<PublicValue?>>, val size: Long)