package org.vitrivr.cottontail.core.database

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * A definition class for a Cottontail DB column be it in a persistent or in-memory context. Specifies all the properties
 * of such and facilitates validation of [Value]s with respect to the [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.7.0
 */
@Serializable
data class ColumnDef<T : Value>(val name: Name.ColumnName, val type: Types<T>, val nullable: Boolean = true, val primary: Boolean = false, val autoIncrement: Boolean = false) {

    /**
     * Validates a value with regard to this [ColumnDef] return a flag indicating whether validation was passed.
     *
     * @param value The value that should be validated.
     * @return True if value passes validation, false otherwise.
     */
    fun validate(value: Value?): Boolean
        = ((value == null && this.nullable) || (value != null && this.type == value.type))

    /**
     * Creates and returns a [String] representation of this [ColumnDef].
     */
    override fun toString(): String = "$name(type=$type, nullable=$nullable, primary=$primary)"
}