package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.extensions.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sort.SortLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySampleLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.LimitLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotFoundException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * This helper class parses and binds queries issued through the gRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The gRPC query components are bound to Cottontail DB objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [OperatorNode.Logical] tree is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object GrpcQueryBinder {

    /** Default projection operator (star projection). */
    private val DEFAULT_PROJECTION = CottontailGrpc.Projection.newBuilder()
        .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
        .addElements(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*")))
        .build()

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [OperatorNode.Logical]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @return [OperatorNode.Logical]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(query: CottontailGrpc.Query, context: QueryContext): OperatorNode.Logical {
        /* Parse and bind FROM clause; take projection into account (-> alias). */
        val projection = if (query.hasProjection()) { query.projection } else { DEFAULT_PROJECTION }
        var root: OperatorNode.Logical = parseAndBindFrom(query.from, projection, context)

        /* Parse and bind FUNCTION-execution; take projection into account (-> alias). */
        projection.elementsList.filter { it.hasFunction() }.forEach {
            root = this.parseAndBindFunctionProjection(root, it, context)
        }

        /* Parse and bind WHERE-clause. */
        root = if (query.hasWhere()) {
            parseAndBindBooleanPredicate(root, query.where, context)
        } else {
            root
        }

        /* Parse and bind ORDER-clause. */
        root = if (query.hasOrder()) {
            parseAndBindOrder(root, query.order, context)
        } else {
            root
        }

        /* Process LIMIT and SKIP. */
        root = if (query.limit > 0L || query.skip > 0L) {
            LimitLogicalOperatorNode(root, query.limit, query.skip)
        } else {
            root
        }

        /* Process SELECT-clause (projection). */
        root = parseAndBindProjection(root, projection, context)
        context.register(root)
        return root
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(insert: CottontailGrpc.InsertMessage, context: QueryContext) {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context)
            val entityTx = context.txn.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val columns = Array<ColumnDef<*>>(insert.elementsCount) {
                val columnName = insert.elementsList[it].column.fqn()
                entityTx.columnForName(columnName).columnDef
            }
            val values = Array<Binding>(insert.elementsCount) {
                context.bindings.bind(insert.elementsList[it].value.toValue(columns[it].type))
            }

            /* Create and return INSERT-clause. */
            val record = RecordBinding(-1L, columns, values)
            context.register(InsertLogicalOperatorNode(context.nextGroupId(), entity, mutableListOf(record)))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(insert: CottontailGrpc.BatchInsertMessage, context: QueryContext) {
        try {
            /* Parse entity for BATCH INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context)
            val entityTx = context.txn.getTx(entity) as EntityTx

            /* Parse columns to BATCH INSERT. */
            val columns = Array<ColumnDef<*>>(insert.columnsCount) {
                val columnName = insert.columnsList[it].fqn()
                entityTx.columnForName(columnName).columnDef
            }

            /* Parse records to BATCH INSERT. */
            val records: MutableList<Record> = insert.insertsList.map { i ->
                RecordBinding(-1L, columns, Array<Binding>(i.valuesCount) {
                   context.bindings.bind(i.valuesList[it].toValue(columns[it].type))
                })
            }.toMutableList()
            context.register(InsertLogicalOperatorNode(context.nextGroupId(), entity, records))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] and returns the [Binding].
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     *
     * @return [RecordBinding]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bindValues(insert: CottontailGrpc.InsertMessage, context: QueryContext): RecordBinding {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context)
            val entityTx = context.txn.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val columns = Array<ColumnDef<*>>(insert.elementsCount) {
                val columnName = insert.elementsList[it].column.fqn()
                entityTx.columnForName(columnName).columnDef
            }
            val values = Array<Binding>(insert.elementsCount) {
                context.bindings.bind(insert.elementsList[it].value.toValue(columns[it].type))
            }
            return RecordBinding(-1L, columns, values)
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(update: CottontailGrpc.UpdateMessage, context: QueryContext) {
        try {
            /* Parse FROM-clause. */
            var root = parseAndBindFrom(update.from, DEFAULT_PROJECTION, context)
            if (root !is EntityScanLogicalOperatorNode) {
                throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
            }
            val entity: EntityTx = root.entity

            /* Parse values to update. */
            val values = update.updatesList.map {
                val column = root.findUniqueColumnForName(it.column.fqn())
                val value = when (it.value.expCase) {
                    CottontailGrpc.Expression.ExpCase.LITERAL -> context.bindings.bind(it.value.literal.toValue(column.type))
                    CottontailGrpc.Expression.ExpCase.COLUMN -> context.bindings.bind(root.findUniqueColumnForName(it.value.column.fqn()))
                    CottontailGrpc.Expression.ExpCase.EXP_NOT_SET,
                    null -> throw QueryException.QuerySyntaxException("")
                }
                column to value
            }

            /* Create WHERE-clause. */
            root = if (update.hasWhere()) {
                parseAndBindBooleanPredicate(root, update.where, context)
            } else {
                root
            }

            /* Create and return UPDATE-clause. */
            context.register(UpdateLogicalOperatorNode(root, entity, values))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     *
     * @return [OperatorNode.Logical]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(delete: CottontailGrpc.DeleteMessage, context: QueryContext) {
        /* Parse FROM-clause. */
        val from = parseAndBindFrom(delete.from, DEFAULT_PROJECTION, context)
        if (from !is EntityScanLogicalOperatorNode) {
            throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
        }
        val entity: EntityTx = from.entity
        var root: OperatorNode.Logical = from

        /* Create WHERE-clause. */
        root = if (delete.hasWhere()) {
            parseAndBindBooleanPredicate(root, delete.where, context)
        } else {
            root
        }

        /* Create and return DELETE-clause. */
        context.register(DeleteLogicalOperatorNode(root, entity))
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @param projection The [CottontailGrpc.Projection] object.
     * @param context The [QueryContext] used for binding.
     *
     * @return The resulting [OperatorNode.Logical].
     */
    private fun parseAndBindFrom(from: CottontailGrpc.From, projection: CottontailGrpc.Projection, context: QueryContext): OperatorNode.Logical = try {
        val projectionMap = projection.elementsList.filter { it.hasColumn() }.map { it.column.fqn() to if (it.hasAlias()) { it.alias.fqn()} else { null } }.toMap()
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.SCAN -> {
                val entity = parseAndBindEntity(from.scan.entity, context)
                val entityTx = context.txn.getTx(entity) as EntityTx
                val fetch = entityTx.listColumns().map {
                    val column = it.columnDef
                    val name = projectionMap[column.name] ?: column.name
                    name to column
                }
                EntityScanLogicalOperatorNode(context.nextGroupId(), entity = entityTx, fetch = fetch)
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.sample.entity, context)
                val entityTx = context.txn.getTx(entity) as EntityTx
                val fetch = entityTx.listColumns().map {
                    val column = it.columnDef
                    val name = projectionMap[column.name] ?: column.name
                    name to column
                }
                EntitySampleLogicalOperatorNode(context.nextGroupId(), entity = entityTx, fetch = fetch, p = from.sample.probability, seed = from.sample.seed)
            }
            CottontailGrpc.From.FromCase.SUBSELECT -> bind(from.subSelect, context) /* Sub-select. */
            else -> throw QueryException.QuerySyntaxException("Invalid or missing FROM-clause in query.")
        }
    } catch (e: DatabaseException) {
        throw QueryException.QueryBindException("Failed to bind FROM due to database error: ${e.message}")
    }

    /**
     * Parses the given [CottontailGrpc.EntityName] and returns the corresponding [Entity].
     *
     * @param entity [CottontailGrpc.EntityName] to parse.
     * @param context The [QueryContext] used for query binding.

     * @return [Entity] that matches [CottontailGrpc.EntityName]
     */
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName, context: QueryContext): Entity = try {
        val name = entity.fqn()
        val catalogueTx = context.txn.getTx(context.catalogue) as CatalogueTx
        val schemaTx = context.txn.getTx(catalogueTx.schemaForName(name.schema())) as SchemaTx
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
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(input: OperatorNode.Logical, where: CottontailGrpc.Where, context: QueryContext): OperatorNode.Logical {
        val predicate = when (where.predicateCase) {
            CottontailGrpc.Where.PredicateCase.ATOMIC -> parseAndBindAtomicBooleanPredicate(input, where.atomic, context)
            CottontailGrpc.Where.PredicateCase.COMPOUND -> parseAndBindCompoundBooleanPredicate(input, where.compound, context)
            CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
            null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        }

        /* Generate FilterLogicalNodeExpression and return it. */
        val subQuery = predicate.atomics.filter { it.dependsOn > 0 }.map { context[it.dependsOn] }
        if (subQuery.isNotEmpty()) {
            return FilterOnSubSelectLogicalOperatorNode(predicate, input, *subQuery.toTypedArray())
        } else {
            return FilterLogicalOperatorNode(input, predicate)
        }
    }


    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [BooleanPredicate.Compound].
     */
    private fun parseAndBindCompoundBooleanPredicate(input: OperatorNode.Logical, compound: CottontailGrpc.CompoundBooleanPredicate, context: QueryContext): BooleanPredicate.Compound {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(input, compound.aleft, context)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(input, compound.cleft, context)
            else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(input, compound.aright, context)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(input, compound.cright, context)
            else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        return try {
            BooleanPredicate.Compound(ConnectionOperator.valueOf(compound.op.name), left, right)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${compound.op.name}' is not a valid connection operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param atomic The [CottontailGrpc.AtomicBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate.Atomic].
     */
    private fun parseAndBindAtomicBooleanPredicate(input: OperatorNode.Logical, atomic: CottontailGrpc.AtomicBooleanPredicate, context: QueryContext): BooleanPredicate.Atomic {
        /* Parse and bind column name to input */
        val left = context.bindings.bind(input.findUniqueColumnForName(atomic.left.fqn()))
        var dependsOn = 0
        val right: List<Binding> = when (atomic.right.operandCase) {
            CottontailGrpc.AtomicBooleanOperand.OperandCase.EXPRESSIONS -> atomic.right.expressions.expressionList.map {
                when(it.expCase) {
                    CottontailGrpc.Expression.ExpCase.COLUMN ->  context.bindings.bind(input.findUniqueColumnForName(it.column.fqn()))
                    CottontailGrpc.Expression.ExpCase.LITERAL -> context.bindings.bind(it.literal.toValue())
                    else -> throw QueryException.QuerySyntaxException("Failed to parse right operand for atomic boolean predicate.")
                }
            }
            CottontailGrpc.AtomicBooleanOperand.OperandCase.QUERY -> {
                val subQuery = this.bind(atomic.right.query, context)
                dependsOn = subQuery.groupId
                if (atomic.op == CottontailGrpc.ComparisonOperator.IN) {
                    emptyList<Binding>()
                } else {
                    listOf(context.bindings.bindNull(left.type))
                }
            }
            else -> throw QueryException.QuerySyntaxException("Failed to parse operand for atomic boolean predicate.")
        }
        return BooleanPredicate.Atomic(bindOperator(atomic.op, left, right), atomic.not, dependsOn)
    }

    /**
     *
     */
    private fun bindOperator(operator: CottontailGrpc.ComparisonOperator, left: Binding, right: List<Binding>) = when (operator) {
        CottontailGrpc.ComparisonOperator.ISNULL -> ComparisonOperator.IsNull(left)
        CottontailGrpc.ComparisonOperator.EQUAL -> ComparisonOperator.Binary.Equal(left, right[0])
        CottontailGrpc.ComparisonOperator.GREATER -> ComparisonOperator.Binary.Greater(left, right[0])
        CottontailGrpc.ComparisonOperator.LESS -> ComparisonOperator.Binary.LessEqual(left, right[0])
        CottontailGrpc.ComparisonOperator.GEQUAL -> ComparisonOperator.Binary.GreaterEqual(left, right[0])
        CottontailGrpc.ComparisonOperator.LEQUAL -> ComparisonOperator.Binary.LessEqual(left, right[0])
        CottontailGrpc.ComparisonOperator.LIKE -> {
            val r = right[0]
            if (r is Binding.Literal && r.value is StringValue) {
                r.value = LikePatternValue.forValue((r.value as StringValue).value)
                ComparisonOperator.Binary.Like(left, r)
            } else {
                throw QueryException.QuerySyntaxException("LIKE operator expects a literal, parseable string value as right operand.")
            }
        }
        CottontailGrpc.ComparisonOperator.MATCH -> {
            val r = right[0]
            if (r is Binding.Literal && r.value is StringValue) {
                r.value = LucenePatternValue((r.value as StringValue).value)
                ComparisonOperator.Binary.Like(left, r)
            } else {
                throw QueryException.QuerySyntaxException("MATCH operator expects a literal, parseable string value as right operand.")
            }
        }
        CottontailGrpc.ComparisonOperator.BETWEEN -> {
            var rightLower = right[0]
            var rightUpper = right[1]
            if (rightLower is Binding.Literal && rightUpper is Binding.Literal) {
                try {
                    if (right[0].value!! > right[1].value!!) { /* Normalize order for literal bindings. */
                        rightLower = right[1]
                        rightUpper = right[0]
                    }
                } catch (e: NullPointerException) {
                    throw QueryException.QuerySyntaxException("BETWEEN operator expects two non-null, literal values as right operands.")
                }
            }
            ComparisonOperator.Between(left, rightLower, rightUpper)
        }
        CottontailGrpc.ComparisonOperator.IN -> ComparisonOperator.In(left, right.filterIsInstance<Binding.Literal>().toMutableList())
        CottontailGrpc.ComparisonOperator.UNRECOGNIZED -> throw QueryException.QuerySyntaxException("Operator $operator is not a valid comparison operator for a boolean predicate!")
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Function]-clause
     *
     * @param input The [OperatorNode.Logical] on which to perform the [Function].
     * @param element The [CottontailGrpc.Function] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindFunctionProjection(input: OperatorNode.Logical, element: CottontailGrpc.Projection.ProjectionElement, context: QueryContext): OperatorNode.Logical {
        require(element.hasFunction()) { "Given project element has not specified a function. This is a programmer's error!" }
        val refs = element.function.argumentsList.mapIndexed { i, a ->
            when (a.expCase) {
                CottontailGrpc.Expression.ExpCase.LITERAL -> context.bindings.bind(a.literal.toValue())
                CottontailGrpc.Expression.ExpCase.COLUMN -> context.bindings.bind(input.findUniqueColumnForName(a.column.fqn()))
                CottontailGrpc.Expression.ExpCase.EXP_NOT_SET,
                null -> throw QueryException.QuerySyntaxException("Function argument at position $i is malformed.")
            }
        }
        val signature = Signature.Closed<Value>(element.function.name, refs.map { it.type }.toTypedArray())
        val functionObject = try {
            context.catalogue.functions.obtain(signature)
        } catch (e: FunctionNotFoundException) {
            throw QueryException.QueryBindException("Desired distance function $signature could not be found!")
        }
        return FunctionProjectionLogicalOperatorNode(input, functionObject, refs, element.alias.fqn())
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Order]-clause
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param order The [CottontailGrpc.Order] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindOrder(input: OperatorNode.Logical, order: CottontailGrpc.Order, context: QueryContext): OperatorNode.Logical {
        val sortOn = order.componentsList.map { input.findUniqueColumnForName(it.column.fqn()) to SortOrder.valueOf(it.direction.toString()) }
        return SortLogicalOperatorNode(input, sortOn)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param projection The [CottontailGrpc.Projection] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [SelectProjectionLogicalOperatorNode].
     */
    private fun parseAndBindProjection(input: OperatorNode.Logical, projection: CottontailGrpc.Projection, context: QueryContext): OperatorNode.Logical {
        val fields = projection.elementsList.mapIndexed { i,p ->
            val name = when (p.projCase) {
                CottontailGrpc.Projection.ProjectionElement.ProjCase.COLUMN -> p.column.fqn()
                CottontailGrpc.Projection.ProjectionElement.ProjCase.FUNCTION -> if (p.hasAlias()) { p.alias.fqn() } else { Name.ColumnName(p.function.name) }
                CottontailGrpc.Projection.ProjectionElement.ProjCase.PROJ_NOT_SET,
                null -> throw QueryException.QuerySyntaxException("Projection element at position $i is malformed (column or function missing).")
            }
            if (p.hasAlias()) {
                name to p.alias.fqn()
            } else {
                name to null
            }
        }
        val type = try {
            Projection.valueOf(projection.op.name)
        } catch (e: java.lang.IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
        }

        /* Return logical node expression for projection. */
        return when (type) {
            Projection.SELECT,
            Projection.SELECT_DISTINCT -> SelectProjectionLogicalOperatorNode(input, type, fields)
            Projection.COUNT -> CountProjectionLogicalOperatorNode(input, fields)
            Projection.EXISTS -> ExistsProjectionLogicalOperatorNode(input, fields)
            Projection.SUM,
            Projection.MAX,
            Projection.MIN,
            Projection.MEAN -> AggregatingProjectionLogicalOperatorNode(input, type, fields)
            else -> throw QueryException.QuerySyntaxException("Project of type $type is currently not supported.")
        }
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