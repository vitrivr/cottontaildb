package org.vitrivr.cottontail.server.grpc.helper

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.predicates.KnnPredicateHint
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

/**
 * Extension function that generates the [KnnPredicateHint] for the given [CottontailGrpc.KnnHint].
 *
 * @param entity The [Entity] the given [KnnPredicateHint] is evaluted for.
 * @return Fully qualified name for the given [KnnPredicateHint]
 */
fun CottontailGrpc.KnnHint.toHint(entity: Entity): KnnPredicateHint? = when (this.hintCase) {
    CottontailGrpc.KnnHint.HintCase.INDEXINEXACT -> KnnPredicateHint.KnnInexactPredicateHint
    CottontailGrpc.KnnHint.HintCase.NOINDEX -> KnnPredicateHint.KnnNoIndexPredicateHint
    CottontailGrpc.KnnHint.HintCase.INDEXTYPE -> KnnPredicateHint.KnnIndexTypePredicateHint(IndexType.valueOf(this.indexType.name))
    CottontailGrpc.KnnHint.HintCase.INDEXNAME -> KnnPredicateHint.KnnIndexNamePredicateHint(entity.name.index(this.indexName))
    CottontailGrpc.KnnHint.HintCase.HINT_NOT_SET -> null
    null -> null
}