package org.vitrivr.cottontail.ui.model.results

/**
 * A generic [Resultset] as returned by the Thumper API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Resultset(val columns: List<Column>, val values: List<Array<Any?>>, val size: Long)