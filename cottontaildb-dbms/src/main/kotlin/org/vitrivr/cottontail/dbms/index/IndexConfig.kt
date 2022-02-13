package org.vitrivr.cottontail.dbms.index

/**
 * An [Index] configuration object.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexConfig {
    /**
     * Converts this [NoIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [NoIndexConfig].
     */
    fun toMap(): Map<String, String>
}