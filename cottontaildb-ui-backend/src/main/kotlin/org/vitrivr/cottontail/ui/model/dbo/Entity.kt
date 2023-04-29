package org.vitrivr.cottontail.ui.model.dbo

/**
 * An [Entity] in the Cottontail DB data model.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Entity (val name: String, val columns: List<Column> = emptyList(), val indexes: List<Index> = emptyList())