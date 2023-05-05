package org.vitrivr.cottontail.ui.model.dbo

import kotlinx.serialization.Serializable

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
enum class DboType {
    SCHEMA,
    ENTITY,
    COLUMN,
    INDEX
}