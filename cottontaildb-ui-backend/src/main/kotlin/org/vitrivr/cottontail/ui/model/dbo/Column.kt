package org.vitrivr.cottontail.ui.model.dbo

import org.vitrivr.cottontail.client.language.basics.Type

/**
 * A [Column] in the Thumper data model.
 *
 * @version 1.0.0
 */
data class Column(val name: String, val type: Type, val length: Int, val nullable: Boolean, val autoIncrement: Boolean = false)