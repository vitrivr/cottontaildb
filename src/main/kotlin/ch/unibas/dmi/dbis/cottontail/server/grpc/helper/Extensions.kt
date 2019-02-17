package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException


/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @retunr Fully qualified name for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Entity.fqn(): String = "${this.schema.name}.${this.name}"



