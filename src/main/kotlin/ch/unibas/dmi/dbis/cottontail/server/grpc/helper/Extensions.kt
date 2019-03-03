package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Schema.fqn(): String = this.name

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Entity.fqn(): String = "${this.schema.name}.${this.name}"

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Index.fqn(): String = "${this.entity.schema.name}.${this.entity.name}.${this.name}"

