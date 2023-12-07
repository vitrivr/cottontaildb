package org.vitrivr.cottontail.utilities.extensions

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.EntityName].
 *
 * @return [Name.SchemaName] for the given [CottontailGrpc.EntityName]
 */
fun CottontailGrpc.SchemaName.fqn(): Name.SchemaName = Name.SchemaName(this.name.lowercase())

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.FunctionName].
 *
 * @return [Name.FunctionName] for the given [CottontailGrpc.FunctionName]
 */
fun CottontailGrpc.FunctionName.fqn(): Name.FunctionName = Name.FunctionName(this.name.lowercase())

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.EntityName].
 *
 * @return [Name.EntityName] for the given [CottontailGrpc.EntityName]
 */
fun CottontailGrpc.EntityName.fqn(): Name.EntityName = Name.EntityName(this.schema.name.lowercase(), this.name.lowercase())

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.IndexName].
 *
 * @return [Name.IndexName] for the given [CottontailGrpc.IndexName]
 */
fun CottontailGrpc.IndexName.fqn(): Name.IndexName = Name.IndexName(this.entity.schema.name.lowercase(), this.entity.name.lowercase(), this.name.lowercase())

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.ColumnName].
 *
 * @return [Name.ColumnName] for the given [CottontailGrpc.ColumnName]
 */
fun CottontailGrpc.ColumnName.fqn(): Name.ColumnName = if (this.hasEntity()) {
    Name.ColumnName(this.entity.schema.name.lowercase(), this.entity.name.lowercase(), this.name.lowercase())
} else {
    Name.ColumnName(this.name.lowercase())
}

/**
 * Extension function that generates the [CottontailGrpc.SchemaName] for the given [Name.SchemaName].
 *
 * @return [CottontailGrpc.SchemaName] for the given [Name.SchemaName].
 */
fun Name.SchemaName.proto() = CottontailGrpc.SchemaName.newBuilder().setName(this.simple).build()

/**
 * Extension function that generates the [CottontailGrpc.EntityName] for the given [Name.EntityName].
 *
 * @return [CottontailGrpc.EntityName] for the given [Name.EntityName].
 */
fun Name.EntityName.proto(): CottontailGrpc.EntityName = CottontailGrpc.EntityName.newBuilder().setName(this.simple).setSchema(this.schema().proto()).build()

/**
 * Extension function that generates the [CottontailGrpc.From] for the given [Name.EntityName].
 *
 * @return [CottontailGrpc.SchemaName] for the given [Name.SchemaName].
 */
fun Name.EntityName.protoFrom(): CottontailGrpc.From = CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(this.proto())).build()

/**
 * Extension function that generates the [CottontailGrpc.IndexName] for the given [Name.IndexName].
 *
 * @return [CottontailGrpc.IndexName] for the given [Name.IndexName]
 */
fun Name.IndexName.proto() = CottontailGrpc.IndexName.newBuilder().setEntity(this.entity().proto()).setName(this.simple).build()

/**
 * Extension function that generates the [CottontailGrpc.ColumnName] for the given [Name.ColumnName].
 *
 * @return [CottontailGrpc.ColumnName] for the given [Name.ColumnName]
 */
fun Name.ColumnName.proto(): CottontailGrpc.ColumnName {
    val name =  CottontailGrpc.ColumnName.newBuilder().setName(this.simple)
    val entityName = this.entity()
    if (entityName != null) {
        val schemaName = this.schema()
        name.entityBuilder.name = entityName.simple
        if (schemaName != null) {
            name.entityBuilder.schemaBuilder.name = schemaName.simple
        }
    }
    return name.build()
}