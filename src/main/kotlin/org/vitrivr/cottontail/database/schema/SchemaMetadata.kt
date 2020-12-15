package org.vitrivr.cottontail.database.schema

import org.vitrivr.cottontail.model.basics.Name

/**
 * Metadata regarding a [Schema]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class SchemaMetadata(val name: Name.EntityName, val created: Long, val modified: Long)