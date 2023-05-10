package org.vitrivr.cottontail.client.language.dql

import org.vitrivr.cottontail.client.language.basics.*
import org.vitrivr.cottontail.client.language.basics.expression.Column
import org.vitrivr.cottontail.client.language.basics.expression.Expression
import org.vitrivr.cottontail.client.language.basics.predicate.Predicate
import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType

/**
 * A query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Suppress("UNCHECKED_CAST")
class Query(entity: String? = null): LanguageFeature() {
    /** Internal [CottontailGrpc.Query.Builder]. */
    val builder: CottontailGrpc.QueryMessage.Builder = CottontailGrpc.QueryMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [Query].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): Query {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [Query].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Query {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [Query]
     *
     * @return The size in bytes of this [Query].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a SELECT projection for a column to this [Query]. Call this method repeatedly to add multiple projections.
     *
     * @param column The name of the column to select.
     * @param alias The column alias. This is optional.
     * @return [Query]
     */
    fun select(column: String, alias: String? = null): Query = select(Column(column), alias)

    /**
     * Adds a SELECT projection to this [Query]. Call this method repeatedly to add multiple projections.
     *
     * Calling this method on a [Query] with a projection other than SELECT, will reset the previous projection.
     *
     * @param expression The [Expression] to execute.
     * @param alias The column alias. This is optional.
     * @return [Query]
     */
    fun select(expression: Expression, alias: String? = null): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.SELECT) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT
        }
        val element = builder.addElementsBuilder()
        element.expression = expression.toGrpc()
        if (alias != null) {
            element.alias = alias.parseColumn()
        }
        return this
    }

    /**
     * Adds a SELECT DISTINCT projection to this [Query]. Call this method repeatedly to add multiple projections.
     *
     * Calling this method on a [Query] with a projection other than SELECT DISTINCT, will reset the previous projection.
     *
     * @param column The name of the column to select.
     * @param alias The column alias. This is optional.
     * @return [Query]
     */
    fun distinct(column: String, alias: String? = null): Query = distinct(Column(column), alias)

    /**
     * Adds a SELECT DISTINCT projection to this [Query]. Call this method repeatedly to add multiple projections.
     *
     * Calling this method on a [Query] with a projection other than SELECT DISTINCT, will reset the previous projection.
     *
     * @param expression The [Expression] to execute.
     * @param alias The column alias. This is optional.
     * @return [Query]
     */
    fun distinct(expression: Expression, alias: String? = null): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.SELECT_DISTINCT) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.SELECT_DISTINCT
        }
        val element = builder.addElementsBuilder()
        element.expression = expression.toGrpc()
        if (alias != null) {
            element.alias = alias.parseColumn()
        }
        return this
    }

    /**
     * Adds a SELECT COUNT projection to this [Query].
     *
     * Calling this method resets the PROJECTION part of the query.
     *
     * @return [Query]
     */
    fun count(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.COUNT) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.COUNT
        }
        builder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setExpression(CottontailGrpc.Expression.newBuilder().setColumn("*".parseColumn())))
        return this
    }

    /**
     * Converts this [Query] to a MEAN() projection.
     *
     * @return [Query]
     */
    fun mean(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.MEAN) {
            builder.op = CottontailGrpc.Projection.ProjectionOperation.MEAN
        }
        return this
    }

    /**
     * Converts this [Query] to a MIN() projection.
     *
     * @return [Query]
     */
    fun min(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.MIN) {
            builder.op = CottontailGrpc.Projection.ProjectionOperation.MIN
        }
        return this
    }

    /**
     * Converts this [Query] to a MIN() projection.
     *
     * @return [Query]
     */
    fun max(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.MAX) {
            builder.op = CottontailGrpc.Projection.ProjectionOperation.MAX
        }
        return this
    }

    /**
     * Adds a SELECT EXISTS projection to this [Query].
     *
     * Calling this method resets the PROJECTION part of the query.
     *
     * @return [Query]
     */
    fun exists(): Query {
        val builder = this.builder.queryBuilder.projectionBuilder
        if (builder.op != CottontailGrpc.Projection.ProjectionOperation.EXISTS) {
            builder.clearElements()
            builder.op = CottontailGrpc.Projection.ProjectionOperation.EXISTS
        }
        builder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setExpression(CottontailGrpc.Expression.newBuilder().setColumn("*".parseColumn())))
        return this
    }

    /**
     * Adds a FROM-clause with a SCAN to this [Query]
     *
     * Calling this method resets the FROM part of the query.
     *
     * @param entity The entity to SCAN.
     * @return This [Query]
     */
    fun from(entity: String): Query {
        this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a FROM-clause with a SAMPLE to this [Query]
     *
     * Calling this method resets the FROM part of the query.
     *
     * @param entity The entity to SAMPLE.
     * @param probability The probability between 0.0f and 1.0f that a tuple will be selected.
     * @param seed The random number generator seed for SAMPLE
     * @return This [Query]
     */
    fun sample(entity: String, probability: Float, seed: Long = System.currentTimeMillis()): Query {
        require(probability in 0.0f..1.0f) { "Probability value must be between 0.0 and 1.0f but is $probability." }
        this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setSample(CottontailGrpc.Sample.newBuilder().setEntity(entity.parseEntity()).setSeed(seed).setProbability(probability)))
        return this
    }

    /**
     * Adds a FROM-clause with a SUB SELECT to this [Query]
     *
     * @param query The [Query] to SUB SELECT from.
     * @return This [Query]
     */
    fun from(query: Query): Query {
        require(query != this) { "SUB-SELECT query cannot specify itself."}
        this.builder.queryBuilder.setFrom(CottontailGrpc.From.newBuilder().setQuery(query.builder.queryBuilder))
        return this
    }

    /**
     * Adds a WHERE-clause to this [Query].
     *
     * @param predicate The [Predicate] that specifies the conditions that need to be met.
     * @return This [Query]
     */
    infix fun where(predicate: Predicate): Query {
        this.builder.queryBuilder.clearWhere()
        this.builder.queryBuilder.whereBuilder.setPredicate(predicate.toGrpc())
        return this
    }

    /**
     * Adds a kNN-clause to this [Query] and returns it.
     *
     * Calling this method has side-effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param column The column to apply the kNN to
     * @param k The k parameter in the kNN
     * @param distance The distance metric to use.
     * @param query Query vector to use.
     * @param weight Weight vector to use; this is not supported anymore!
     * @return This [Query]
     */
    @Deprecated("Deprecated since version 0.13.0; use nns() function instead!", replaceWith = ReplaceWith("nns"))
    fun knn(column: String, k: Int, distance: String, query: PublicValue, weight: Any? = null): Query {
        if (weight != null)throw UnsupportedOperationException("Weighted NNS is no longer supported by Cottontail DB. Use weighted distance function with respective arguments instead.")

        /* Calculate distance. */
        distance(column, query, Distances.valueOf(distance.uppercase()),"distance")

        /* Update ORDER BY clause. */
        this.builder.queryBuilder.orderBuilder.addComponents(CottontailGrpc.Order.Component.newBuilder().setColumn(
            CottontailGrpc.ColumnName.newBuilder().setName("distance")
        ).setDirection(Direction.ASC.toGrpc()))

        /* Update LIMIT clause. */
        this.builder.queryBuilder.limit = k.toLong()

        return this
    }

    /**
     * Transforms this [Query] to a Neighbor Search (NS) query and returns it.
     *
     * Calling this method has side effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param probingColumn The column to perform NNS on. Type must be compatible with choice of distance function.
     * @param query Query value to use. Type must be compatible with choice of distance function.
     * @param distance The distance function to use. Function argument must be compatible with column type.
     * @param name The name of the column that holds the calculated distance value.
     * @return This [Query]
     */
    fun distance(probingColumn: String, query: PublicValue, distance: Distances, name: String): Query {
        /* Parse necessary functions. */
        val distanceColumn = name.parseColumn()
        val distanceFunction = CottontailGrpc.Function.newBuilder()
            .setName(distance.toGrpc())
            .addArguments(CottontailGrpc.Expression.newBuilder().setColumn(probingColumn.parseColumn()))
            .addArguments(CottontailGrpc.Expression.newBuilder().setLiteral(query.toGrpc()))

        /* Update projection: Add distance column + alias. */
        this.builder.queryBuilder.projectionBuilder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder()
            .setAlias(distanceColumn)
            .setExpression(CottontailGrpc.Expression.newBuilder().setFunction(distanceFunction)))

        /* Update LIMIT clause. */
        return this
    }

    /**
     * Transforms this [Query] to a Farthest Neighbor Search (FNS) query and returns it.
     *
     * Calling this method has side effects on various aspects of the [Query] (i.e., PROJECTION, ORDER and LIMIT).
     * Most importantly, this function is not idempotent, i.e., calling it multiple times changes the structure of the
     * query, e.g., by adding multiple distance functions. Use [clear] to be on the safe side.
     *
     * @param probingColumn The column to perform fulltext search on.
     * @param query Query [String] value to use. Type must be compatible with choice of distance function.
     * @param name The name of the column that holds the calculated distance value.
     * @return This [Query]
     */
    fun fulltext(probingColumn: String, query: String, name: String): Query {
        val scoreColumn = name.parseColumn()
        val fulltextFunction = CottontailGrpc.Function.newBuilder()
            .setName(CottontailGrpc.FunctionName.newBuilder().setName("fulltext"))
            .addArguments(CottontailGrpc.Expression.newBuilder().setColumn(probingColumn.parseColumn()))
            .addArguments(CottontailGrpc.Expression.newBuilder().setLiteral(CottontailGrpc.Literal.newBuilder().setStringData(query)))

        /* Update projection: Add distance column + alias. */
        this.builder.queryBuilder.projectionBuilder.addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder()
            .setAlias(scoreColumn)
            .setExpression(CottontailGrpc.Expression.newBuilder().setFunction(fulltextFunction)))

        return this
    }

    /**
     * Adds a ORDER BY-clause to this [Query] and returns it
     *
     * @param column The column to order by
     * @param direction The sort [Direction]
     * @return This [Query]
     */
    fun order(column: String, direction: Direction): Query {
        val builder = this.builder.queryBuilder.orderBuilder
        val cBuilder = builder.addComponentsBuilder()
        cBuilder.column = column.parseColumn()
        cBuilder.direction = direction.toGrpc()
        return this
    }

    /**
     * Adds a SKIP-clause in the Cottontail DB query language.
     *
     * @param skip The number of results to skip.
     * @return This [Query]
     */
    fun skip(skip: Long): Query {
        this.builder.queryBuilder.skip = skip
        return this
    }

    /**
     * Adds a LIMIT-clause in the Cottontail DB query language.
     *
     * @param limit The number of results to return at maximum.
     * @return This [Query]
     */
    fun limit(limit: Long): Query {
        this.builder.queryBuilder.limit = limit
        return this
    }

    /**
     * Clears this [Query] making it a green slate object that can be used to build a [Query]
     *
     * @return This [Query]
     */
    fun clear(): Query {
        this.builder.queryBuilder.clear()
        return this
    }

    /**
     * Sets a hint to the query planner that instructs it not use any indexes.
     *
     * @return This [Query]
     */
    fun disallowIndex(): Query {
        this.builder.metadataBuilder.indexHintBuilder.disallow = true
        return this
    }

    /**
     * Sets a hint to the query planner that instructs it to use a specific index.
     *
     * @param index The name of the index to use.
     * @return This [Query]
     */
    fun useIndex(index: String): Query {
        val parsed = index.parseIndex() /* Sanity check. */
        this.builder.metadataBuilder.indexHintBuilder.name = parsed.name
        return this
    }

    /**
     * Sets a hint to the query planner that instructs it to use a specific index type.
     *
     * @param type The name of the index to use.
     * @return This [Query]
     */
    fun useIndexType(type: String): Query {
        val parsed = IndexType.valueOf(type.uppercase())
        this.builder.metadataBuilder.indexHintBuilder.type = parsed
        return this
    }

    /**
     * Uses a specific cost policy for this [Query].
     *
     * @param wio The relative importance of IO costs.
     * @param wcpu The relative importance of CPU costs.
     * @param wmem The relative importance of Memory costs.
     * @param wq The relative importance of Quality costs.
     * @return This [Query]
     */
    fun usePolicy(wio: Float = 1.0f, wcpu: Float = 1.0f, wmem: Float = 1.0f, wq: Float = 1.0f): Query {
        require(wio in 0.0f..1.0f) { "Cost policy weights must be in the range [0.0, 1.0]." }
        require(wcpu in 0.0f..1.0f) { "Cost policy weights must be in the range [0.0, 1.0]." }
        require(wmem in 0.0f..1.0f) { "Cost policy weights must be in the range [0.0, 1.0]." }
        require(wq in 0.0f..1.0f) { "Cost policy weights must be in the range [0.0, 1.0]." }
        this.builder.metadataBuilder.policyHintBuilder.setWeightIo(wio).setWeightCpu(wcpu).setWeightMemory(wmem).setWeightAccuracy(wq)
        return this
    }

    /**
     * Sets the 'no optimisation' hint in the [Query].
     *
     * @return This [Query]
     */
    fun disallowOptimisation(): Query {
        this.builder.metadataBuilder.noOptimiseHint = true
        return this
    }

    /**
     * Sets a hint to the query planner that disallows any form of intra-query parallelism
     *
    * @return This [Query]
     */
    fun disallowParallelism(): Query = limitParallelism(1)

    /**
     * Sets a hint to the query planner that limits the amount of allowed intra-query parallelism.
     *
     * @param max The maximum amount of parallelism to allow for.
     * @return This [Query]
     */
    fun limitParallelism(max: Int): Query {
        this.builder.metadataBuilder.parallelHintBuilder.limit = max
        return this
    }
}