package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.model.basics.Name

/**
 * Metadata regarding a [Schema]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityMetadata(val name: Name.EntityName, val created: Long, var modified: Long)