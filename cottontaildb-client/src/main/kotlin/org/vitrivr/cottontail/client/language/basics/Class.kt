package org.vitrivr.cottontail.client.language.basics

import kotlinx.serialization.Serializable

/**
 * The database object (dbo) classes supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
enum class Class {
    SCHEMA,
    ENTITY,
    COLUMN,
    INDEX;
}
