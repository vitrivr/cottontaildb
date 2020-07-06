package org.vitrivr.cottontail.server.grpc.helper

import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Name

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Schema.fqn(): Name.SchemaName = Name.SchemaName(this.name)

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Entity.fqn(): Name.EntityName = Name.EntityName(this.schema.name, this.name)

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Index.fqn(): Name.IndexName = Name.IndexName(this.entity.schema.name, this.entity.name, this.name)
