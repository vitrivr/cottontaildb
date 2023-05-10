package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.core.database.Name
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

/**
 * Converts a [Name.SchemaName] into an [CottontailGrpc.SchemaName]
 *
 * @return [CottontailGrpc.SchemaName]
 */
fun Name.SchemaName.proto(): CottontailGrpc.SchemaName = CottontailGrpc.SchemaName.newBuilder().setName(this.simple).build()

/**
 * Converts a [Name.EntityName] into an [CottontailGrpc.EntityName]
 *
 * @return [CottontailGrpc.EntityName]
 */
fun Name.EntityName.proto(): CottontailGrpc.EntityName = CottontailGrpc.EntityName.newBuilder()
    .setName(this.entityName)
    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(this.schemaName)).build()

/**
 * Converts a [Name.IndexName] into an [CottontailGrpc.IndexName]
 *
 * @return [CottontailGrpc.IndexName]
 */
fun Name.IndexName.proto(): CottontailGrpc.IndexName = CottontailGrpc.IndexName.newBuilder()
    .setName(this.indexName)
    .setEntity(CottontailGrpc.EntityName.newBuilder().setName(this.entityName)
    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(this.schemaName))).build()

/**
 * Converts a [Name.ColumnName] into an [CottontailGrpc.ColumnName]
 *
 * @return [CottontailGrpc.ColumnName]
 */
fun Name.ColumnName.proto(): CottontailGrpc.ColumnName = CottontailGrpc.ColumnName.newBuilder()
    .setName(this.columnName)
    .setEntity(CottontailGrpc.EntityName.newBuilder().setName(this.entityName)
    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(this.schemaName))).build()

/**
 * Converts a [Name.FunctionName] into an [CottontailGrpc.FunctionName]
 *
 * @return [CottontailGrpc.FunctionName]
 */
fun Name.FunctionName.proto(): CottontailGrpc.FunctionName
    = CottontailGrpc.FunctionName.newBuilder().setName(this.functionName).build()