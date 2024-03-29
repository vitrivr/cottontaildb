package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Parses a [CottontailGrpc.FunctionName] into a parsable [Name.FunctionName]
 *
 * @return [Name.FunctionName]
 */
fun CottontailGrpc.FunctionName.parse(): Name.FunctionName = Name.FunctionName.create(this.name)

/**
 * Parses a [CottontailGrpc.SchemaName] into a parsable [Name.SchemaName]
 *
 * @return [Name.SchemaName]
 */
fun CottontailGrpc.SchemaName.parse(): Name.SchemaName = Name.SchemaName.create(this.name)

/**
 * Parses a [CottontailGrpc.EntityName] into a [Name.EntityName]
 *
 * @return [Name.EntityName]
 */
fun CottontailGrpc.EntityName.parse(): Name.EntityName = Name.EntityName.create(this.schema.name, this.name)

/**
 * Parses a [CottontailGrpc.EntityName] into a [Name.IndexName]
 *
 * @return [Name.IndexName]
 */
fun CottontailGrpc.IndexName.parse(): Name.IndexName = Name.IndexName.create(this.entity.schema.name, this.entity.name, this.name)

/**
 * Parses a [CottontailGrpc.ColumnName] into a parsable [Name.ColumnName]
 *
 * @return [Name.ColumnName]
 */
fun CottontailGrpc.ColumnName.parse(): Name.ColumnName = Name.ColumnName.create(
    if (this.hasEntity() && this.entity.hasSchema()) this.entity.schema.name else Name.WILDCARD,
    if (this.hasEntity()) this.entity.name else Name.WILDCARD,
    this.name
)

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
    .setName(this.entity)
    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(this.schema)).build()

/**
 * Converts a [Name.IndexName] into an [CottontailGrpc.IndexName]
 *
 * @return [CottontailGrpc.IndexName]
 */
fun Name.IndexName.proto(): CottontailGrpc.IndexName = CottontailGrpc.IndexName.newBuilder()
    .setName(this.index)
    .setEntity(CottontailGrpc.EntityName.newBuilder().setName(this.entity)
    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(this.schema))).build()

/**
 * Converts a [Name.ColumnName] into an [CottontailGrpc.ColumnName]
 *
 * @return [CottontailGrpc.ColumnName]
 */
fun Name.ColumnName.proto(): CottontailGrpc.ColumnName = CottontailGrpc.ColumnName.newBuilder()
    .setName(this.column)
    .setEntity(CottontailGrpc.EntityName.newBuilder().setName(this.entity)
    .setSchema(CottontailGrpc.SchemaName.newBuilder().setName(this.schema))).build()

/**
 * Converts a [Name.FunctionName] into an [CottontailGrpc.FunctionName]
 *
 * @return [CottontailGrpc.FunctionName]
 */
fun Name.FunctionName.proto(): CottontailGrpc.FunctionName
    = CottontailGrpc.FunctionName.newBuilder().setName(this.function).build()