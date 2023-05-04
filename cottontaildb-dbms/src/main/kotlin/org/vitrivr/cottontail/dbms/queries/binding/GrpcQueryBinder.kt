package org.vitrivr.cottontail.dbms.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotFoundException
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Types
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
import org.vitrivr.cottontail.utilities.extensions.fqn
import org.vitrivr.cottontail.utilities.extensions.toValue

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
                this@QueryContext.bindings.bind(ColumnDef(it.first.second, function.function.signature.returnType, function.canBeNull))
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
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity)
            val entityTx = entity.newTx(this@QueryContext)

            /* Parse columns to INSERT. */
            val columns = Array<ColumnDef<*>>(insert.elementsCount) {
                val columnName = insert.elementsList[it].column.fqn()
                entityTx.columnForName(columnName).columnDef
            }
            val values = Array(insert.elementsCount) {
                val literal = insert.elementsList[it].value
                if (literal.dataCase == CottontailGrpc.Literal.DataCase.DATA_NOT_SET) {
                    this@QueryContext.bindings.bindNull(columns[it].type)
                } else {
                    this@QueryContext.bindings.bind(literal.toValue(columns[it].type))
                }
            }

            /* Create and return INSERT-clause. */
            val record = RecordBinding(-1L, columns, values, this@QueryContext.bindings)
            return InsertLogicalOperatorNode(this@QueryContext.nextGroupId(), entityTx, mutableListOf(record))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
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
        try {
            /* Parse entity for BATCH INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity)
            val entityTx = entity.newTx(this@QueryContext)

            /* Parse columns to BATCH INSERT. */
            val columns = Array<ColumnDef<*>>(insert.columnsCount) {
                val columnName = insert.columnsList[it].fqn()
                entityTx.columnForName(columnName).columnDef
            }

            /* Parse records to BATCH INSERT. */
            val records: MutableList<Record> = insert.insertsList.map { ins ->
                RecordBinding(-1L, columns, Array(ins.valuesCount) { i ->
                    val literal = ins.valuesList[i]
                    if (literal.dataCase == CottontailGrpc.Literal.DataCase.DATA_NOT_SET) {
                        this@QueryContext.bindings.bindNull(columns[i].type)
                    } else {
                        this@QueryContext.bindings.bind(literal.toValue(columns[i].type))
                    }
                }, this@QueryContext.bindings)
            }.toMutableList()
            return InsertLogicalOperatorNode(this@QueryContext.nextGroupId(), entityTx, records)
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
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
        try {
            /* Parse FROM-clause. */
            var root = parseAndBindFrom(update.from, parseProjectionColumns(DEFAULT_PROJECTION))
            if (root !is EntityScanLogicalOperatorNode) {
                throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
            }
            val entity: EntityTx = root.entity

            /* Parse values to update. */
            val values = update.updatesList.map {
                val column = root.findUniqueColumnForName(it.column.fqn())
                val value = when (it.value.expCase) {
                    CottontailGrpc.Expression.ExpCase.LITERAL -> {
                        if (it.value.literal.dataCase == CottontailGrpc.Literal.DataCase.DATA_NOT_SET) {
                            this@QueryContext.bindings.bindNull(column.type)
                        } else {
                            this@QueryContext.bindings.bind(it.value.literal.toValue(column.type))
                        }
                    }
                    CottontailGrpc.Expression.ExpCase.COLUMN -> this@QueryContext.bindings.bind(root.findUniqueColumnForName(it.value.column.fqn()))
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
            return UpdateLogicalOperatorNode(root, entity, values)
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
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
        return DeleteLogicalOperatorNode(root, entity)
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
                    if (name == null || name.key.columnName == Name.WILDCARD) {
                        this@QueryContext.bindings.bind(def) to def
                    } else {
                        this@QueryContext.bindings.bind(def.copy(name = name.key)) to def
                    }
                }
                EntityScanLogicalOperatorNode(this@QueryContext.nextGroupId(), entityTx, fetch)
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.sample.entity)
                val entityTx = entity.newTx(this@QueryContext)
                val fetch = entityTx.listColumns().map { def ->
                    val name = columns.entries.singleOrNull { c -> c.value is Name.ColumnName && (c.value as Name.ColumnName).matches(def.name) }
                    if (name == null || name.key.columnName == Name.WILDCARD) {
                        this@QueryContext.bindings.bind(def) to def
                    } else {
                        this@QueryContext.bindings.bind(def.copy(name = name.key)) to def
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
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName): Entity = try {
        val name = entity.fqn()
        val catalogueTx = this@QueryContext.catalogue.newTx(this@QueryContext)
        val schemaTx = catalogueTx.schemaForName(name.schema()).newTx(this@QueryContext)
        schemaTx.entityForName(name)
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.schema}'. Schema does not exist!")
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.entity}'. Entity does not exist!")
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
            parseAndBindPredicate(input, predicate.and.p1)
        )
        CottontailGrpc.Predicate.PredicateCase.OR -> BooleanPredicate.Or(
            parseAndBindPredicate(input, predicate.or.p1),
            parseAndBindPredicate(input, predicate.or.p1)
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
        CottontailGrpc.Predicate.Comparison.Operator.EQUAL -> ComparisonOperator.Binary.Equal(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.NOTEQUAL -> ComparisonOperator.Binary.NotEqual(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.GREATER -> ComparisonOperator.Binary.Greater(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.LESS -> ComparisonOperator.Binary.Less(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.GEQUAL -> ComparisonOperator.Binary.GreaterEqual(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.LEQUAL -> ComparisonOperator.Binary.LessEqual(left, right)
        CottontailGrpc.Predicate.Comparison.Operator.LIKE -> {
            with(MissingRecord) {
                with(this@QueryContext.bindings) {
                    if (right is Binding.Literal && right.getValue() is StringValue) {
                        right.update(LikePatternValue.forValue((right.getValue() as StringValue).value))
                        ComparisonOperator.Binary.Like(left, right)
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
        CottontailGrpc.Expression.ExpCase.LITERAL ->  this@QueryContext.bindings.bind(expression.literal.toValue())
        CottontailGrpc.Expression.ExpCase.LITERALLIST -> this@QueryContext.bindings.bind(expression.literalList.literalList.map { it.toValue() })
        CottontailGrpc.Expression.ExpCase.COLUMN -> this@QueryContext.bindings.bind(input.findUniqueColumnForName(expression.column.fqn()))
        CottontailGrpc.Expression.ExpCase.FUNCTION -> parseAndBindFunction(input, expression.function)
        CottontailGrpc.Expression.ExpCase.QUERY -> {
            val subquery = this.bind(expression.query,)
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
                CottontailGrpc.Expression.ExpCase.LITERAL -> this@QueryContext.bindings.bind(a.literal.toValue())
                CottontailGrpc.Expression.ExpCase.COLUMN -> this@QueryContext.bindings.bind(input.findUniqueColumnForName(a.column.fqn()))
                CottontailGrpc.Expression.ExpCase.FUNCTION ->  parseAndBindFunction(input, a.function)
                else -> throw QueryException.QuerySyntaxException("Function argument at position $i is malformed.")
            }
        }

        /* Try tp resolve signature and obtain function object. */
        val signature = Signature.SemiClosed(function.name.fqn(), arguments.map { Argument.Typed(it.type) }.toTypedArray())
        val functionInstance = try {
            this@QueryContext.catalogue.functions.obtain(signature)
        } catch (e: FunctionNotFoundException) {
            throw QueryException.QueryBindException("Desired function $signature could not be found.")
        }
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
        val sortOn = order.componentsList.map { input.findUniqueColumnForName(it.column.fqn()) to SortOrder.valueOf(it.direction.toString()) }
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
    private fun parseAndBindProjection(input: OperatorNode.Logical, projection: Map<Name.ColumnName, Name?>, op: Projection): OperatorNode.Logical = when (op) {
        Projection.SELECT -> {
            val fields = projection.keys.flatMap { cp ->
                input.columns.filter { c -> cp.matches(c.name) }.ifEmpty { throw QueryException.QueryBindException("Column $cp could not be found in output.") }
            }.map {
                it.name
            }
            SelectProjectionLogicalOperatorNode(input, fields)
        }
        Projection.SELECT_DISTINCT -> {
            val fields = projection.keys.flatMap { cp ->
                input.columns.filter { c -> cp.matches(c.name) }.ifEmpty { throw QueryException.QueryBindException("Column $cp could not be found in output.") }
            }.map {
                it.name to true
            }
            SelectDistinctProjectionLogicalOperatorNode(input, fields, this@QueryContext.catalogue.config)
        }
        Projection.COUNT -> {
            val columnName = projection.keys.first()
            val columnDef = ColumnDef(Name.ColumnName(columnName.schemaName, columnName.entityName,"count(${columnName.columnName})"), Types.Long, false)
            CountProjectionLogicalOperatorNode(input, this@QueryContext.bindings.bind(columnDef))
        }
        Projection.COUNT_DISTINCT -> {
            val fields = projection.keys.flatMap { cp ->
                input.columns.filter { c -> cp.matches(c.name) }.ifEmpty { throw QueryException.QueryBindException("Column $cp could not be found in output.") }
            }.map {
                it.name to true
            }
            val columnDef = ColumnDef(Name.ColumnName(fields.first().first.schemaName, fields.first().first.entityName,"count(${fields.joinToString(",") { it.first.columnName }})"), Types.Long, false)
            CountProjectionLogicalOperatorNode(SelectDistinctProjectionLogicalOperatorNode(input, fields, this@QueryContext.catalogue.config), this@QueryContext.bindings.bind(columnDef))
        }
        Projection.EXISTS -> {
            val columnName = projection.keys.first()
            val columnDef = ColumnDef(Name.ColumnName(columnName.schemaName, columnName.entityName, "exists(${columnName.columnName})"), Types.Long, false)
            ExistsProjectionLogicalOperatorNode(input, this@QueryContext.bindings.bind(columnDef))
        }
        Projection.SUM,
        Projection.MAX,
        Projection.MIN,
        Projection.MEAN -> {
            val fields = projection.keys.flatMap { cp ->
                input.columns.filter { c -> cp.matches(c.name) }.ifEmpty { throw QueryException.QueryBindException("Column $cp could not be found in output.") }
            }.map {
                it.name
            }
            AggregatingProjectionLogicalOperatorNode(input, op, fields)
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
                        e.hasAlias() -> e.alias.fqn()
                        simplify -> Name.ColumnName(e.expression.column.name)
                        else -> e.expression.column.fqn()
                    }

                    /* Check for uniqueness and assign. */
                    if (map.contains(finalName)) {
                        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Duplicate projection element $finalName at index $i.")
                    }
                    map[finalName] = e.expression.column.fqn()
                }
                CottontailGrpc.Expression.ExpCase.FUNCTION -> {
                    /* Sanity check; star projections can't have aliases. */
                    val finalName = when {
                        e.hasAlias() -> e.alias.fqn()
                        else -> Name.ColumnName(e.expression.function.name.name)
                    }

                    /* Check for uniqueness and assign. */
                    if (map.contains(finalName)) {
                        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Duplicate projection element $finalName at index $i.")
                    }
                    map[finalName] = e.expression.function.name.fqn()
                }
                CottontailGrpc.Expression.ExpCase.LITERAL -> {
                    if(!e.hasAlias()) throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Projection element at index $i is malformed.")
                    val finalName = e.alias.fqn()
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
     * @return [ColumnDef] that uniquely matches the [Name.ColumnName]
     */
    private fun OperatorNode.Logical.findUniqueColumnForName(name: Name.ColumnName): ColumnDef<*> {
        val candidates = this.findColumnsForName(name)
        if (candidates.isEmpty()) throw QueryException.QueryBindException("Could not find column '$name' in input.")
        if (candidates.size > 1) throw QueryException.QueryBindException("Multiple candidates for column '$name' in input.")
        return candidates.first()
    }

    /**
     * Tries to find and return all [ColumnDef]s that matches the given [Name.ColumnName] in this [OperatorNode.Logical].
     *
     * @param name [Name.ColumnName] to look for.
     * @return List of [ColumnDef] that  match the [Name.ColumnName]
     */
    private fun OperatorNode.Logical.findColumnsForName(name: Name.ColumnName): List<ColumnDef<*>> =
        this.columns.filter { name.matches(it.name) }
}