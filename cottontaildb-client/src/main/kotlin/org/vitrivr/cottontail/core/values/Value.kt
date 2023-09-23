package org.vitrivr.cottontail.core.values

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Value] that is part of Cottontail DBs public interface.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
sealed interface Value: Comparable<Value> {
    /** The [Types] of this [Value]. */
    val type: Types<*>

    /** Logical size of this [Value]. */
    val logicalSize: Int
        get() = this.type.logicalSize

    /** Physical size of this [Value] in bytes. */
    val physicalSize: Int
        get() = this.type.physicalSize

    /**
     * Compares two [Value]s. Returns true, if they are equal, and false otherwise.
     *
     * This method is required because it is currently not possible to override equals() in Kotlin inline classes. Once this changes, this method should be removed.
     *
     * @param other Value to compare to.
     * @return true if equal, false otherwise.
     */
    fun isEqual(other: Value): Boolean

    /**
     * Converts this [Value] to a [CottontailGrpc.Literal.Builder] gRCP representation.
     *
     * @return [CottontailGrpc.Literal.Builder]
     */
    fun toGrpc(): CottontailGrpc.Literal.Builder
}