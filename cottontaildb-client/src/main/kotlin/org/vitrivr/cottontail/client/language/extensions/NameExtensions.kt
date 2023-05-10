package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.lang.StringBuilder


/**
 * Parses a [CottontailGrpc.ColumnName] into a parsable [String]
 *
 * @return [String]
 */
fun CottontailGrpc.ColumnName.fqn(): String {
    val builder = StringBuilder()
    if (this.hasEntity()) {
        if (this.entity.hasSchema()) {
            builder.append(this.entity.schema.name)
            builder.append(".")
        }
        builder.append(this.entity.name)
        builder.append(".")
    }
    builder.append(this.name)
    return builder.toString()
}