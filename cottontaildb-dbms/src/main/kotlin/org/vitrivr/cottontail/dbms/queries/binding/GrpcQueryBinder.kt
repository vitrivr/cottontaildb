package org.vitrivr.cottontail.dbms.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.client.language.extensions.parse
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.toType
import org.vitrivr.cottontail.core.toValue
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.function.FunctionLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.UpdateLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.projection.*
import org.vitrivr.cottontail.dbms.queries.operators.logical.sort.SortLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntitySampleLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.LimitLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.SkipLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This helper class parses and binds queries issued through the gRPC endpoint.
 *
 * The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The gRPC query components are bound to Cottontail DB database objects.
 * 3) Construction of a [OperatorNode.Logical] tree from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
object GrpcQueryBinder {

    /** Default projection operator (star projection). */
    private val DEFAULT_PROJECTION = CottontailGrpc.Projection.newBuilder()
        .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
        .addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setExpression(CottontailGrpc.Expression.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*"))))
        .build()

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [OperatorNode.Logical]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @return [OperatorNode.Logical]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    context(QueryContext)
    fun bind(query: CottontailGrpc.Query): OperatorNode.Logical {
        /* Parse SELECT-clause (projection); this clause is important because of aliases. */
        val projection = if (query.hasProjection()) { query.projection } else { DEFAULT_PROJECTION }
        val columns = this.parseProjectionColumns(projection)

        /** Parse FROM-clause and take projection into account. */
        var root: OperatorNode.Logical = parseAndBindFrom(query.from, columns)

        /* Parse and bind functions in projection. */
        columns.entries.map {
            Pair(it.value, it.key) /* Reverse association. */
        }.zip(projection.elementsList).filter {
            it.first.first is Name.FunctionName
        }.forEach {
            val function = this.parseAndBindFunction(root, it.second.expression.function)
            root = FunctionLogicalOperatorNode(
                root,
                function,
                this@QueryContext.bindings.bind(
                    ColumnDef(it.first.second, function.function.signature.returnType, function.canBeNull),
                    null
                )
            )
        }

        /* Parse and bind WHERE-clause. */
        if (query.hasWhere()) {
            root = parseAndBindWhere(root, query.where)
        }

        /* Parse and bind ORDER-clause. */
        if (query.hasOrder()) {
            root = parseAndBindOrder(root, query.order)
        }

        /* Process SKIP. */
        if (query.skip > 0L) {
            root = SkipLogicalOperatorNode(root, query.skip)
        }

        /* Process LIMIT. */
        if (query.limit > 0L) {
            root = LimitLogicalOperatorNode(root, query.limit)
        }

        /* Process SELECT-clause (projection). */
        return parseAndBindProjection(root, columns, Projection.valueOf(projection.op.toString()))
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @return [InsertLogicalOperatorNode].
     */
    context(QueryContext)
    fun bind(insert: CottontailGrpc.InsertMessage): InsertLogicalOperatorNode {
        /* Parse entity for INSERT. */
        val entity = parseAndBindEntity(insert.from.scan.entity)
        val entityTx = entity.newTx(this@QueryContext)

        /* Parse columns to INSERT. */
        val columns = Array<ColumnDef<*>>(insert.elementsCount) {
            val columnName = insert.elementsList[it].column.parse()
            entityTx.columnForName(columnName).columnDef
        }
        val values = Array(insert.elementsCount) {
            val literal = insert.elementsList[it].value.toValue()
            if (literal == null) {
                this@QueryContext.bindings.bindNull(columns[it].type)
            } else {
                this@QueryContext.bindings.bind(literal)
            }
        }

        /* Create and return INSERT-clause. */
        try {
            val record = TupleBinding(-1L, columns, values, this@QueryContext.bindings)
            return InsertLogicalOperatorNode(this@QueryContext.nextGroupId(), this@QueryContext, entityTx, mutableListOf(record))
        } catch (e: IllegalArgumentException) {
            throw DatabaseException.ValidationException("Provided data could not be bound to INSERT due to validation error: ${e.message}")
        }
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @return [InsertLogicalOperatorNode]
     */
    context(QueryContext)
    fun bind(insert: CottontailGrpc.BatchInsertMessage): InsertLogicalOperatorNode {
        /* Parse entity for BATCH INSERT. */
        val entity = parseAndBindEntity(insert.from.scan.entity)
        val entityTx = entity.newTx(this@QueryContext)

        /* Parse columns to BATCH INSERT. */
        val columns = Array<ColumnDef<*>>(insert.columnsCount) {
            val columnName = insert.columnsList[it].parse()
            entityTx.columnForName(columnName).columnDef
        }

        /* Parse records to BATCH INSERT. */
        try {
            val tuples: MutableList<Tuple> = insert.insertsList.map { ins ->
                TupleBinding(-1L, columns, Array(ins.valuesCount) { i ->
                    val literal = ins.valuesList[i].toValue()
                    if (literal == null) {
                        this@QueryContext.bindings.bindNull(columns[i].type)
                    } else {
                        this@QueryContext.bindings.bind(literal)
                    }
                }, this@QueryContext.bindings)
            }.toMutableList()
            return InsertLogicalOperatorNode(this@QueryContext.nextGroupId(), this@QueryContext, entityTx, tuples)
        } catch (e: IllegalArgumentException) {
            throw DatabaseException.ValidationException("Provided data could not be bound to BATCH INSERT due to validation error: ${e.message}")
        }
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @return [UpdateLogicalOperatorNode]
     */
    context(QueryContext)
    fun bind(update: CottontailGrpc.UpdateMessage): UpdateLogicalOperatorNode {
        /* Parse FROM-clause. */
        var root = parseAndBindFrom(update.from, parseProjectionColumns(DEFAULT_PROJECTION))
        if (root !is EntityScanLogicalOperatorNode) {
            throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
        }
        val entity: EntityTx = root.entity

        /* Parse values to update. */
        val values = update.updatesList.map {
            val column = root.findUniqueColumnForName(it.column.parse())
            val value = when (it.value.expCase) {
                CottontailGrpc.Expression.ExpCase.LITERAL -> {
                    val v = it.value.literal.toValue()
                    if (v == null) {
                        this@QueryContext.bindings.bindNull(it.value.literal.toType())
                    } else {
                        this@QueryContext.bindings.bind(v)
                    }
                }
                CottontailGrpc.Expression.ExpCase.COLUMN -> root.findUniqueColumnForName(it.value.column.parse())
                CottontailGrpc.Expression.ExpCase.EXP_NOT_SET ->  this@QueryContext.bindings.bindNull(column.type)
                CottontailGrpc.Expression.ExpCase.FUNCTION -> throw QueryException.QuerySyntaxException("Function expressions are not yet not supported as values in update statements.") /* TODO. */
                else -> throw QueryException.QuerySyntaxException("Failed to bind value for column '${column}': Unsupported expression!")
            }
            column to value
        }

        /* Create WHERE-clause. */
        root = if (update.hasWhere()) {
            parseAndBindWhere(root, update.where)
        } else {
            root
        }

        /* Create and return UPDATE-clause. */
        return UpdateLogicalOperatorNode(root, this@QueryContext, entity, values)
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     *
     * @return [DeleteLogicalOperatorNode]
     */
    context(QueryContext)
    fun bind(delete: CottontailGrpc.DeleteMessage): DeleteLogicalOperatorNode {
        /* Parse FROM-clause. */
        val from = parseAndBindFrom(delete.from, parseProjectionColumns(DEFAULT_PROJECTION))
        if (from !is EntityScanLogicalOperatorNode) {
            throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
        }
        val entity: EntityTx = from.entity
        var root: OperatorNode.Logical = from

        /* Create WHERE-clause. */
        root = if (delete.hasWhere()) {
            parseAndBindWhere(root, delete.where)
        } else {
            root
        }

        /* Create and return DELETE-clause. */
        return DeleteLogicalOperatorNode(root, this@QueryContext, entity)
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @param columns The list of [Name.ColumnName] as per parsed projection-clause. Used to determine aliases.*
     * @return The resulting [OperatorNode.Logical].
     */
    context(QueryContext)
    private fun parseAndBindFrom(from: CottontailGrpc.From, columns: Map<Name.ColumnName, Name?>): OperatorNode.Logical = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.SCAN -> {
                val entity = parseAndBindEntity(from.scan.entity)
                val entityTx = entity.newTx(this@QueryContext)
                val fetch = entityTx.listColumns().map { def ->
                    val name = columns.entries.singleOrNull { c -> c.value is Name.ColumnName && (c.value as Name.ColumnName).matches(def.name) }
                    if (name == null || name.key.column == Name.WILDCARD) {
                        this@QueryContext.bindings.bind(def, def)
                    } else {
                        this@QueryContext.bindings.bind(def.copy(name = name.key), def)
                    }
                }
                EntityScanLogicalOperatorNode(this@QueryContext.nextGroupId(), entityTx, fetch)
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.sample.entity)
                val entityTx = entity.newTx(this@QueryContext)
                val fetch = entityTx.listColumns().map { def ->
                    val name = columns.entries.singleOrNull { c -> c.value is Name.ColumnName && (c.value as Name.ColumnName).matches(def.name) }
                    if (name == null || name.key.column == Name.WILDCARD) {
                        this@QueryContext.bindings.bind(def, def)
                    } else {
                        this@QueryContext.bindings.bind(def.copy(name = name.key), def)
                    }
                }
                EntitySampleLogicalOperatorNode(this@QueryContext.nextGroupId(), entityTx, fetch, from.sample.probability, from.sample.seed)
            }
            CottontailGrpc.From.FromCase.QUERY -> bind(from.query) /* Sub-select. */
            else -> throw QueryException.QuerySyntaxException("Invalid or missing FROM-clause in query.")
        }
    } catch (e: DatabaseException) {
        throw QueryException.QueryBindException("Failed to bind FROM due to database error: ${e.message}")
    }

    /**
     * Parses the given [CottontailGrpc.EntityName] and returns the corresponding [Entity].
     *
     * @param entity [CottontailGrpc.EntityName] to parse.
     * @return [Entity] that matches [CottontailGrpc.EntityName]
     */
    context(QueryContext)
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName): Entity {
        val name = entity.parse()
        val catalogueTx = this@QueryContext.catalogue.newTx(this@QueryContext)
        val schemaTx = catalogueTx.schemaForName(name.schema()).newTx(this@QueryContext)
        return schemaTx.entityForName(name)
    }

    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param where The [CottontailGrpc.Where] object.
     * @return The resulting [OperatorNode.Logical].
     */
    context(QueryContext)
    private fun parseAndBindWhere(input: OperatorNode.Logical, where: CottontailGrpc.Where): OperatorNode.Logical {
        val predicate = this.parseAndBindPredicate(input, where.predicate)
        return FilterLogicalOperatorNode(input, predicate)
    }

    /**
     * Parses and binds a [CottontailGrpc.Predicate.
     *
     * @param input The [OperatorNode.Logical] in the current context
     * @param predicate The [CottontailGrpc.Predicate] object.
     * @return The resulting [BooleanPredicate].
     */
    context(QueryContext)
    private fun parseAndBindPredicate(input: OperatorNode.Logical, predicate: CottontailGrpc.Predicate): BooleanPredicate = when (predicate.predicateCase) {
        CottontailGrpc.Predicate.PredicateCase.LITERAL -> BooleanPredicate.Literal(predicate.literal.value)
        CottontailGrpc.Predicate.PredicateCase.ISNULL -> BooleanPredicate.Literal(predicate.literal.value)
        CottontailGrpc.Predicate.PredicateCase.COMPARISON -> parseAndBindComparison(input, predicate.comparison)
        CottontailGrpc.Predicate.PredicateCase.AND -> BooleanPredicate.And(
            parseAndBindPredicate(input, predicate.and.p1),
            parseAndBindPredicate(input, predicate.and.p2)
        )
        CottontailGrpc.Predicate.PredicateCase.OR -> BooleanPredicate.Or(
            parseAndBindPredicate(input, predicate.or.p1),
            parseAndBindPredicate(input, predicate.or.p2)
        )
        CottontailGrpc.Predicate.PredicateCase.NOT -> BooleanPredicate.Not(
            parseAndBindPredicate(input, predicate.not.p),
        )
        else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param comparison The [CottontailGrpc.Predicate.Comparison] object.
     *
     * @return The resulting [BooleanPredicate.Comparison].
     */
    context(QueryContext)
    private fun parseAndBindComparison(input: OperatorNode.Logical, comparison: CottontailGrpc.Predicate.Comparison): BooleanPredicate.Comparison {
        /* Parse and bind column name to input */
        val left = this.parseAndBindExpression(input, comparison.lexp)
        val right = this.parseAndBindExpression(input, comparison.rexp)
        return BooleanPredicate.Comparison(parseAndBindOperator(comparison.operator, left, right))
    }

    /**
     * Parses and binds a [CottontailGrpc.Predicate.Comparison.Operator] and returns the associated [ComparisonOperator].
     *
     * @param operator The [CottontailGrpc.Predicate.Comparison.Operator] to bind.
     * @param left The left [Binding]
     * @param right The right [Binding]'s or an empty list.
     * @return [ComparisonOperator] that corresponds to the [CottontailGrpc.Predicate.Comparison.Operator].
     */
    context(QueryContext)
    private fun parseAndBindOperator(operator: CottontailGrpc.Predicate.Comparison.Operator, left: Binding, right: Binding) = when (operator) {
        CottontailGrpc.Predicate.Comparison.Operator.EQUAL -> ComparisonOperator.Equal(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.NOTEQUAL -> ComparisonOperator.NotEqual(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.GREATER -> ComparisonOperator.Greater(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.LESS -> ComparisonOperator.Less(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.GEQUAL -> ComparisonOperator.GreaterEqual(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.LEQUAL -> ComparisonOperator.LessEqual(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.LIKE -> {
            with(MissingTuple) {
                with(this@QueryContext.bindings) {
                    if (right is Binding.Literal && right.getValue() is StringValue) {
                        right.update(LikePatternValue.forValue((right.getValue() as StringValue).value))
                        ComparisonOperator.Like(left, right)
                    } else {
                        throw QueryException.QuerySyntaxException("LIKE operator expects a literal, parseable string value as right operand.")
                    }
                }
            }
        }
        CottontailGrpc.Predicate.Comparison.Operator.BETWEEN -> try {
            ComparisonOperator.Between(left, right)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException(e.message ?: "Right operand of IN operator is malformed.")
        }
        CottontailGrpc.Predicate.Comparison.Operator.IN -> try {
            ComparisonOperator.In(left, right)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException(e.message ?: "Right operand of IN operator is malformed.")
        }
        else -> throw QueryException.QuerySyntaxException("Operator $operator is not a valid comparison operator for a boolean predicate!")
    }

    /**
     * Parses and binds a [expression: CottontailGrpc.Expression] and returns the associated [Binding].
     *
     * @param input The [OperatorNode.Logical] in context
     * @param expression The [CottontailGrpc.Expression] to parse and bind
     * @return [ComparisonOperator] that corresponds to the [CottontailGrpc.Predicate.Comparison.Operator].
     */
    context(QueryContext)
    private fun parseAndBindExpression(input: OperatorNode.Logical, expression: CottontailGrpc.Expression): Binding = when(expression.expCase) {
        CottontailGrpc.Expression.ExpCase.LITERAL -> {
            val v = expression.literal.toValue()
            if (v == null) {
                this@QueryContext.bindings.bindNull(expression.literal.toType())
            } else {
                this@QueryContext.bindings.bind(v)
            }
        }
        CottontailGrpc.Expression.ExpCase.LITERALLIST -> this@QueryContext.bindings.bind(expression.literalList.literalList.mapNotNull { it.toValue() })
        CottontailGrpc.Expression.ExpCase.COLUMN -> input.findUniqueColumnForName(expression.column.parse())
        CottontailGrpc.Expression.ExpCase.FUNCTION -> parseAndBindFunction(input, expression.function)
        CottontailGrpc.Expression.ExpCase.QUERY -> {
            val subquery = this.bind(expression.query)
            this@QueryContext.register(subquery) /* Register sub-query. */
            Binding.Subquery(subquery.groupId, subquery.columns.first())
        }
        else -> throw QueryException.QuerySyntaxException("Expression of type ${expression.expCase} is not valid!")
    }

    /**
     * Parses and binds a [CottontailGrpc.Function]
     *
     * @param input The [OperatorNode.Logical] on which to perform the [Function].
     * @param function The (unparsed) [CottontailGrpc.Function] object.
     *
     * @return The resulting [Binding.Function].
     */
    context(QueryContext)
    private fun parseAndBindFunction(input: OperatorNode.Logical, function: CottontailGrpc.Function): Binding.Function {
        val arguments = function.argumentsList.mapIndexed { i, a ->
            when (a.expCase) {
                CottontailGrpc.Expression.ExpCase.LITERAL -> {
                    val v = a.literal.toValue()
                    if (v == null) {
                        this@QueryContext.bindings.bindNull(a.literal.toType())
                    } else {
                        this@QueryContext.bindings.bind(v)
                    }
                }
                CottontailGrpc.Expression.ExpCase.COLUMN -> input.findUniqueColumnForName(a.column.parse())
                CottontailGrpc.Expression.ExpCase.FUNCTION ->  parseAndBindFunction(input, a.function)
                else -> throw QueryException.QuerySyntaxException("Function argument at position $i is malformed.")
            }
        }

        /* Try tp resolve signature and obtain function object. */
        val signature = Signature.SemiClosed(function.name.parse(), arguments.map { Argument.Typed(it.type) }.toTypedArray())
        val functionInstance = this@QueryContext.catalogue.functions.obtain(signature)
        return this@QueryContext.bindings.bind(functionInstance, arguments)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Order]-clause
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param order The [CottontailGrpc.Order] object.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindOrder(input: OperatorNode.Logical, order: CottontailGrpc.Order): OperatorNode.Logical {
        val sortOn = order.componentsList.map { input.findUniqueColumnForName(it.column.parse()) to SortOrder.valueOf(it.direction.toString()) }
        return SortLogicalOperatorNode(input, sortOn)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param projection Map of output [Name.ColumnName] to input [Name].
     * @return The resulting [SelectProjectionLogicalOperatorNode].
     */
    context(QueryContext)
    private fun parseAndBindProjection(input: OperatorNode.Logical, projection: Map<Name.ColumnName, Name?>, op: Projection): OperatorNode.Logical {
        val fields = projection.keys.flatMap { cp ->
            input.columns.filter { c -> cp.matches(c.column.name) }.ifEmpty {
                throw QueryException.QueryBindException("Column $cp could not be found in output.")
            }
        }
        return when (op) {
            Projection.SELECT -> SelectProjectionLogicalOperatorNode(input, fields)
            Projection.SELECT_DISTINCT -> SelectDistinctProjectionLogicalOperatorNode(input, fields, this@QueryContext.catalogue.config)
            Projection.SUM,
            Projection.MAX,
            Projection.MIN,
            Projection.MEAN -> AggregatingProjectionLogicalOperatorNode(input, op, fields)
            Projection.COUNT -> {
                val columnDef = ColumnDef(Name.ColumnName.create(fields.first().column.name.schema, fields.first().column.name.entity, "count(${fields.first().column.name.column})"), Types.Long, false)
                CountProjectionLogicalOperatorNode(input, this@QueryContext.bindings.bind(columnDef, null))
            }
            Projection.EXISTS -> {
                val columnDef = ColumnDef(Name.ColumnName.create(fields.first().column.name.schema, fields.first().column.name.entity, "exists(${fields.first().column.name.column}"), Types.Long, false)
                ExistsProjectionLogicalOperatorNode(input, this@QueryContext.bindings.bind(columnDef, null))
            }
            Projection.COUNT_DISTINCT -> {
                val columnDef = ColumnDef(Name.ColumnName.create(fields.first().column.name.schema, fields.first().column.name.entity, "count(${fields.joinToString(",") { it.column.name.column }})"), Types.Long, false)
                CountProjectionLogicalOperatorNode(SelectDistinctProjectionLogicalOperatorNode(input, fields, this@QueryContext.catalogue.config), this@QueryContext.bindings.bind(columnDef, null))
            }
        }
    }


    /**
     * Parses the list of [Name.ColumnName] in the [CottontailGrpc.Projection] element and returns a map of the source [Name.ColumnName] and/or
     * [Name.FunctionName] the resulting [Name.ColumnName] resolving both simplification and aliases.
     *
     * @param projection The [CottontailGrpc.Projection] element to parse.
     * @param simplify Whether names should be simplified, i.e., simple name instead of FQN should be used.
     * @return Mapping of resulting [Name.ColumnName] to source [Name]
     */
    context(QueryContext)
    private fun parseProjectionColumns(projection: CottontailGrpc.Projection, simplify: Boolean = false): Map<Name.ColumnName, Name?> {
        val map = Object2ObjectLinkedOpenHashMap<Name.ColumnName, Name>()
        projection.elementsList.forEachIndexed { i, e ->
            when (e.expression.expCase) {
                CottontailGrpc.Expression.ExpCase.COLUMN -> {
                    /* Sanity check; star projections can't have aliases. */
                    if (e.expression.column.name == "*" && e.hasAlias()) {
                        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Cannot assign alias to star-projection at index $i.")
                    }

                    /* Determine final name of column. */
                    val finalName = when {
                        e.hasAlias() -> e.alias.parse()
                        simplify -> Name.ColumnName.create(e.expression.column.name)
                        else -> e.expression.column.parse()
                    }

                    /* Check for uniqueness and assign. */
                    if (map.contains(finalName)) {
                        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Duplicate projection element $finalName at index $i.")
                    }
                    map[finalName] = e.expression.column.parse()
                }
                CottontailGrpc.Expression.ExpCase.FUNCTION -> {
                    /* Sanity check; star projections can't have aliases. */
                    val finalName = when {
                        e.hasAlias() -> e.alias.parse()
                        else -> Name.ColumnName.create(e.expression.function.name.name)
                    }

                    /* Check for uniqueness and assign. */
                    if (map.contains(finalName)) {
                        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Duplicate projection element $finalName at index $i.")
                    }
                    map[finalName] = e.expression.function.name.parse()
                }
                CottontailGrpc.Expression.ExpCase.LITERAL -> {
                    if(!e.hasAlias()) throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Projection element at index $i is malformed.")
                    val finalName = e.alias.parse()
                    map[finalName] = null
                }
                else -> throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Projection element at index $i is malformed.")
            }
        }
        return map
    }

    /**
     * Tries to find and return a [ColumnDef] that matches the given [Name.ColumnName] in
     * this [OperatorNode.Logical]. The match must be unique!
     *
     * @param name [Name.ColumnName] to look for.
     * @return [Binding.Column] that uniquely matches the [Name.ColumnName]
     */
    private fun OperatorNode.Logical.findUniqueColumnForName(name: Name.ColumnName): Binding.Column {
        val candidates = this.columns.filter { name.matches(it.column.name) }
        if (candidates.isEmpty()) throw QueryException.QueryBindException("Could not find column '$name' in input.")
        if (candidates.size > 1) throw QueryException.QueryBindException("Multiple candidates for column '$name' in input.")
        return candidates.first()
    }
}