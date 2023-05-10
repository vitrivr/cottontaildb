package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Parses a [String] into an [CottontailGrpc.EntityName]
 *
 * @return [CottontailGrpc.EntityName]
 */
fun String.parseSchema(): CottontailGrpc.SchemaName {
    val split = this.lowercase().split('.')
    return when (split.size) {
        1 -> CottontailGrpc.SchemaName.newBuilder().setName(split[0]).build()
        2 -> CottontailGrpc.SchemaName.newBuilder().setName(split[1]).build()
        else -> throw IllegalStateException("Cottontail DB schema names can consist of at most two components separated by a dot: [warren.]<schema>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.EntityName]
 *
 * @return [CottontailGrpc.EntityName]
 */
fun String.parseEntity(): CottontailGrpc.EntityName {
    val split = this.lowercase().split('.')
    return when (split.size) {
        2 -> CottontailGrpc.EntityName.newBuilder().setName(split[1]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[0])).build()
        3 -> CottontailGrpc.EntityName.newBuilder().setName(split[2]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[1])).build()
        else -> throw IllegalStateException("Cottontail DB entity names must consist of at least two and at most three components separated by a dot: [warren.]<schema>.<entity>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.IndexName]
 *
 * @return [CottontailGrpc.IndexName]
 */
fun String.parseIndex(): CottontailGrpc.IndexName {
    val split = this.lowercase().split('.')
    return when (split.size) {
        3 -> CottontailGrpc.IndexName.newBuilder().setName(split[2]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[1]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[0]))).build()
        4 -> CottontailGrpc.IndexName.newBuilder().setName(split[3]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[2]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[1]))).build()
        else -> throw IllegalStateException("Cottontail DB index names must consist of at least three and at most four components separated by a dot: [warren.]<schema>.<entity>.<index>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.ColumnName]
 *
 * @return [CottontailGrpc.ColumnName]
 */
fun String.parseColumn(): CottontailGrpc.ColumnName {
    val split = this.lowercase().split('.')
    return when (split.size) {
        1 -> CottontailGrpc.ColumnName.newBuilder().setName(split[0]).build()
        3 -> CottontailGrpc.ColumnName.newBuilder().setName(split[2]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[1]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[0]))).build()
        4 -> CottontailGrpc.ColumnName.newBuilder().setName(split[3]).setEntity(CottontailGrpc.EntityName.newBuilder().setName(split[2]).setSchema(CottontailGrpc.SchemaName.newBuilder().setName(split[1]))).build()
        else -> throw IllegalStateException("Cottontail DB column names can consist of one, three or four components separated by a dot: [warren.]<schema>.<entity>.<column>")
    }
}

/**
 * Parses a [String] into an [CottontailGrpc.FunctionName]
 *
 * @return [CottontailGrpc.FunctionName]
 */
fun String.parseFunction(): CottontailGrpc.FunctionName = CottontailGrpc.FunctionName.newBuilder().setName(this).build()