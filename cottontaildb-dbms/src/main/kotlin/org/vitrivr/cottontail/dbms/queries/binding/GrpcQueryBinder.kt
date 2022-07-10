package org.vitrivr.cottontail.dbms.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotFoundException
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.function.FunctionLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.UpdateLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.projection.*
import org.vitrivr.cottontail.dbms.queries.operators.logical.sort.SortLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntitySampleLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.LimitLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.transform.SkipLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection
import org.vitrivr.cottontail.dbms.schema.SchemaTx
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
 * @version 2.1.0
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
     * @param context The [DefaultQueryContext] used for binding.
     * @return [OperatorNode.Logical]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    @Suppress("UNCHECKED_CAST")
    fun bind(query: CottontailGrpc.Query, context: DefaultQueryContext): OperatorNode.Logical {
        /* Parse SELECT-clause (projection); this clause is important because of aliases. */
        val projection = if (query.hasProjection()) { query.projection } else { DEFAULT_PROJECTION }
        val columns = this.parseProjectionColumns(projection)

        /** Parse FROM-clause and take projection into account. */
        var root: OperatorNode.Logical = parseAndBindFrom(query.from, columns, context)

        /* Parse and bind functions in projection. */
        columns.entries.map {
            Pair(it.value, it.key) /* Reverse association. */
        }.zip(projection.elementsList).filter {
            it.first.first is Name.FunctionName
        }.forEach {
            root = this.parseAndBindFunction(root, it.second.expression.function, it.first.second, context)
        }

        /* Parse and bind WHERE-clause. */
        if (query.hasWhere()) {
            root = parseAndBindBooleanPredicate(root, query.where, context)
        }

        /* Parse and bind ORDER-clause. */
        if (query.hasOrder()) {
            root = parseAndBindOrder(root, query.order, context)
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
        return parseAndBindProjection(root, columns, Projection.valueOf(projection.op.toString()), context)
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [DefaultQueryContext] used for binding.
     * @return [InsertLogicalOperatorNode].
     */
    fun bind(insert: CottontailGrpc.InsertMessage, context: DefaultQueryContext): InsertLogicalOperatorNode {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context)
            val entityTx = context.txn.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val columns = Array<ColumnDef<*>>(insert.elementsCount) {
                val columnName = insert.elementsList[it].column.fqn()
                entityTx.columnForName(columnName).columnDef
            }
            val values = Array(insert.elementsCount) {
                val literal = insert.elementsList[it].value
                if (literal.dataCase == CottontailGrpc.Literal.DataCase.DATA_NOT_SET) {
                    context.bindings.bindNull(columns[it].type)
                } else {
                    context.bindings.bind(literal.toValue(columns[it].type))
                }
            }

            /* Create and return INSERT-clause. */
            val record = RecordBinding(-1L, columns, values)
            return InsertLogicalOperatorNode(context.nextGroupId(), entityTx, mutableListOf(record))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [DefaultQueryContext] used for binding.
     * @return [InsertLogicalOperatorNode]
     */
    fun bind(insert: CottontailGrpc.BatchInsertMessage, context: DefaultQueryContext): InsertLogicalOperatorNode {
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
            val records: MutableList<Record> = insert.insertsList.map { ins ->
                RecordBinding(-1L, columns, Array(ins.valuesCount) { i ->
                    val literal = ins.valuesList[i]
                    if (literal.dataCase == CottontailGrpc.Literal.DataCase.DATA_NOT_SET) {
                        context.bindings.bindNull(columns[i].type)
                    } else {
                        context.bindings.bind(literal.toValue(columns[i].type))
                    }
                })
            }.toMutableList()
            return InsertLogicalOperatorNode(context.nextGroupId(), entityTx, records)
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @param context The [DefaultQueryContext] used for binding.
     * @return [UpdateLogicalOperatorNode]
     */
    fun bind(update: CottontailGrpc.UpdateMessage, context: DefaultQueryContext): UpdateLogicalOperatorNode {
        try {
            /* Parse FROM-clause. */
            var root = parseAndBindFrom(update.from, parseProjectionColumns(DEFAULT_PROJECTION), context)
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
                            context.bindings.bindNull(column.type)
                        } else {
                            context.bindings.bind(it.value.literal.toValue(column.type))
                        }
                    }
                    CottontailGrpc.Expression.ExpCase.COLUMN -> context.bindings.bind(root.findUniqueColumnForName(it.value.column.fqn()))
                    CottontailGrpc.Expression.ExpCase.EXP_NOT_SET ->  context.bindings.bindNull(column.type)
                    CottontailGrpc.Expression.ExpCase.FUNCTION -> throw QueryException.QuerySyntaxException("Function expressions are not yet not supported as values in update statements.") /* TODO. */
                    else -> throw QueryException.QuerySyntaxException("Failed to bind value for column '${column}': Unsupported expression!")
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
     * @param context The [DefaultQueryContext] used for binding.
     *
     * @return [DeleteLogicalOperatorNode]
     */
    fun bind(delete: CottontailGrpc.DeleteMessage, context: DefaultQueryContext): DeleteLogicalOperatorNode {
        /* Parse FROM-clause. */
        val from = parseAndBindFrom(delete.from, parseProjectionColumns(DEFAULT_PROJECTION), context)
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
        return DeleteLogicalOperatorNode(root, entity)
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @param columns The list of [Name.ColumnName] as per parsed projection-clause. Used to determine aliases.
     * @param context The [DefaultQueryContext] used for binding.
     *
     * @return The resulting [OperatorNode.Logical].
     */
    private fun parseAndBindFrom(from: CottontailGrpc.From, columns: Map<Name.ColumnName, Name>, context: DefaultQueryContext): OperatorNode.Logical = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.SCAN -> {
                val entity = parseAndBindEntity(from.scan.entity, context)
                val entityTx = context.txn.getTx(entity) as EntityTx
                val fetch = entityTx.listColumns().map { def ->
                    val name = columns.entries.singleOrNull { c -> c.value is Name.ColumnName && c.value.matches(def.name) }
                    if (name == null || name.key.columnName == Name.WILDCARD) {
                        context.bindings.bind(def) to def
                    } else {
                        context.bindings.bind(def.copy(name = name.key)) to def
                    }
                }
                EntityScanLogicalOperatorNode(context.nextGroupId(), entityTx, fetch)
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.sample.entity, context)
                val entityTx = context.txn.getTx(entity) as EntityTx
                val fetch = entityTx.listColumns().map { def ->
                    val name = columns.entries.singleOrNull { c -> c.value is Name.ColumnName && c.value.matches(def.name) }
                    if (name == null || name.key.columnName == Name.WILDCARD) {
                        context.bindings.bind(def) to def
                    } else {
                        context.bindings.bind(def.copy(name = name.key)) to def
                    }
                }
                EntitySampleLogicalOperatorNode(context.nextGroupId(), entityTx, fetch, from.sample.probability, from.sample.seed)
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
     * @param context The [DefaultQueryContext] used for query binding.

     * @return [Entity] that matches [CottontailGrpc.EntityName]
     */
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName, context: DefaultQueryContext): Entity = try {
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
     * @param context The [DefaultQueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(input:OperatorNode.Logical, where: CottontailGrpc.Where, context: DefaultQueryContext): OperatorNode.Logical {
        val subqueries = mutableListOf<OperatorNode.Logical>()
        val predicate = when (where.predicateCase) {
            CottontailGrpc.Where.PredicateCase.ATOMIC -> parseAndBindAtomicBooleanPredicate(input, where.atomic, context, subqueries)
            CottontailGrpc.Where.PredicateCase.COMPOUND -> parseAndBindCompoundBooleanPredicate(input, where.compound, context, subqueries)
            CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
            null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        }

        /* Generate FilterLogicalNodeExpression and return it. */
        return if (subqueries.isNotEmpty()) {
            FilterOnSubSelectLogicalOperatorNode(predicate, input, *subqueries.toTypedArray())
        } else {
            FilterLogicalOperatorNode(input, predicate)
        }
    }


    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     * @param context The [DefaultQueryContext] used for query binding.

     * @return The resulting [BooleanPredicate.Compound].
     */
    private fun parseAndBindCompoundBooleanPredicate(input: OperatorNode.Logical, compound: CottontailGrpc.CompoundBooleanPredicate, context: DefaultQueryContext, subqueries: MutableList<OperatorNode.Logical>): BooleanPredicate.Compound {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(input, compound.aleft, context, subqueries)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(input, compound.cleft, context, subqueries)
            else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(input, compound.aright, context, subqueries)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(input, compound.cright, context, subqueries)
            else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        return when(compound.op) {
            CottontailGrpc.ConnectionOperator.AND -> BooleanPredicate.Compound.And(left, right)
            CottontailGrpc.ConnectionOperator.OR -> BooleanPredicate.Compound.Or(left, right)
            else -> throw QueryException.QuerySyntaxException("'${compound.op.name}' is not a valid connection operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param atomic The [CottontailGrpc.AtomicBooleanPredicate] object.
     * @param context The [DefaultQueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate.Atomic].
     */
    private fun parseAndBindAtomicBooleanPredicate(input: OperatorNode.Logical, atomic: CottontailGrpc.AtomicBooleanPredicate, context: DefaultQueryContext, subqueries: MutableList<OperatorNode.Logical>): BooleanPredicate.Atomic {
        /* Parse and bind column name to input */
        val left = context.bindings.bind(input.findUniqueColumnForName(atomic.left.fqn()))
        val right: List<Binding> = when (atomic.right.operandCase) {
            CottontailGrpc.AtomicBooleanOperand.OperandCase.EXPRESSIONS -> atomic.right.expressions.expressionList.map {
                when(it.expCase) {
                    CottontailGrpc.Expression.ExpCase.COLUMN ->  context.bindings.bind(input.findUniqueColumnForName(it.column.fqn()))
                    CottontailGrpc.Expression.ExpCase.LITERAL -> context.bindings.bind(it.literal.toValue())
                    CottontailGrpc.Expression.ExpCase.FUNCTION -> parseAndBindNestedFunction(input, it.function, context)
                    else -> throw QueryException.QuerySyntaxException("Failed to parse right operand for atomic boolean predicate.")
                }
            }
            CottontailGrpc.AtomicBooleanOperand.OperandCase.QUERY -> {
                val subquery = this.bind(atomic.right.query, context)
                subqueries.add(subquery)
                listOf(context.bindings.bind(subquery.groupId, subquery.columns.first()))
            }
            else -> throw QueryException.QuerySyntaxException("Failed to parse operand for atomic boolean predicate.")
        }
        return BooleanPredicate.Atomic(bindOperator(atomic.op, left, right), atomic.not)
    }

    /**
     * Parses and binds a [CottontailGrpc.ComparisonOperator] and returns the associated [ComparisonOperator].
     *
     * @param operator The [CottontailGrpc.ComparisonOperator] to bind.
     * @param left The left [Binding]
     * @param right The right [Binding]'s or an empty list.
     * @return [ComparisonOperator] that corresponds to the [CottontailGrpc.ComparisonOperator].
     */
    private fun bindOperator(operator: CottontailGrpc.ComparisonOperator, left: Binding, right: List<Binding>) = when (operator) {
        CottontailGrpc.ComparisonOperator.ISNULL -> ComparisonOperator.IsNull(left)
        CottontailGrpc.ComparisonOperator.EQUAL -> ComparisonOperator.Binary.Equal(left, right[0])
        CottontailGrpc.ComparisonOperator.GREATER -> ComparisonOperator.Binary.Greater(left, right[0])
        CottontailGrpc.ComparisonOperator.LESS -> ComparisonOperator.Binary.Less(left, right[0])
        CottontailGrpc.ComparisonOperator.GEQUAL -> ComparisonOperator.Binary.GreaterEqual(left, right[0])
        CottontailGrpc.ComparisonOperator.LEQUAL -> ComparisonOperator.Binary.LessEqual(left, right[0])
        CottontailGrpc.ComparisonOperator.LIKE -> {
            val r = right[0]
            if (r is Binding.Literal && r.value is StringValue) {
                r.update(LikePatternValue.forValue((r.value as StringValue).value))
                ComparisonOperator.Binary.Like(left, r)
            } else {
                throw QueryException.QuerySyntaxException("LIKE operator expects a literal, parseable string value as right operand.")
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
     * @param function The (unparsed) [CottontailGrpc.Function] object.
     * @param name The [Name.ColumnName] of the resulting column
     * @param context The [DefaultQueryContext] used for query binding.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindFunction(input: OperatorNode.Logical, function: CottontailGrpc.Function, name: Name.ColumnName, context: DefaultQueryContext): OperatorNode.Logical {
        val arguments = function.argumentsList.mapIndexed { i, a ->
            when (a.expCase) {
                CottontailGrpc.Expression.ExpCase.LITERAL -> context.bindings.bind(a.literal.toValue())
                CottontailGrpc.Expression.ExpCase.COLUMN -> context.bindings.bind(input.findUniqueColumnForName(a.column.fqn()))
                CottontailGrpc.Expression.ExpCase.FUNCTION -> parseAndBindNestedFunction(input, a.function, context)
                else -> throw QueryException.QuerySyntaxException("Function argument at position $i is malformed.")
            }
        }

        /* Try to resolve signature and obtain function object. */
        val signature = Signature.SemiClosed(function.name.fqn(), arguments.map { Argument.Typed(it.type) }.toTypedArray())
        val functionInstance = try {
            context.catalogue.functions.obtain(signature)
        } catch (e: FunctionNotFoundException) {
            throw QueryException.QueryBindException("Desired function $signature could not be found.")
        }
        val functionBinding = context.bindings.bind(functionInstance, arguments)
        val outBinding = context.bindings.bind(ColumnDef(name, functionInstance.signature.returnType, functionBinding.canBeNull))
        return FunctionLogicalOperatorNode(input, functionBinding, outBinding)
    }

    /**
     * Parses and binds a nested [CottontailGrpc.Function]
     *
     * @param input The [OperatorNode.Logical] on which to perform the [Function].
     * @param function The (unparsed) [CottontailGrpc.Function] object.
     * @param context The [DefaultQueryContext] used for query binding.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindNestedFunction(input: OperatorNode.Logical, function: CottontailGrpc.Function, context: DefaultQueryContext): Binding.Function {
        val arguments = function.argumentsList.mapIndexed { i, a ->
            when (a.expCase) {
                CottontailGrpc.Expression.ExpCase.LITERAL -> context.bindings.bind(a.literal.toValue())
                CottontailGrpc.Expression.ExpCase.COLUMN -> context.bindings.bind(input.findUniqueColumnForName(a.column.fqn()))
                CottontailGrpc.Expression.ExpCase.FUNCTION ->  parseAndBindNestedFunction(input, a.function, context)
                else -> throw QueryException.QuerySyntaxException("Function argument at position $i is malformed.")
            }
        }

        /* Try tp resolve signature and obtain function object. */
        val signature = Signature.SemiClosed(function.name.fqn(), arguments.map { Argument.Typed(it.type) }.toTypedArray())
        val functionInstance = try {
            context.catalogue.functions.obtain(signature)
        } catch (e: FunctionNotFoundException) {
            throw QueryException.QueryBindException("Desired function $signature could not be found.")
        }
        return context.bindings.bind(functionInstance, arguments)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Order]-clause
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param order The [CottontailGrpc.Order] object.
     * @param context The [DefaultQueryContext] used for query binding.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindOrder(input: OperatorNode.Logical, order: CottontailGrpc.Order, context: DefaultQueryContext): OperatorNode.Logical {
        val sortOn = order.componentsList.map { input.findUniqueColumnForName(it.column.fqn()) to SortOrder.valueOf(it.direction.toString()) }
        return SortLogicalOperatorNode(input, sortOn)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param projection Map of output [Name.ColumnName] to input [Name].
     * @param context The [DefaultQueryContext] used for query binding.
     *
     * @return The resulting [SelectProjectionLogicalOperatorNode].
     */
private fun parseAndBindProjection(input: OperatorNode.Logical, projection: Map<Name.ColumnName, Name>, op: Projection, context: DefaultQueryContext): OperatorNode.Logical = when (op) {
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
            SelectDistinctProjectionLogicalOperatorNode(input, fields, context.catalogue.config)
        }
        Projection.COUNT -> {
            val columnName = projection.keys.first()
            val columnDef = ColumnDef(Name.ColumnName(columnName.schemaName, columnName.entityName,"count(${columnName.columnName})"), Types.Long, false)
            CountProjectionLogicalOperatorNode(input, context.bindings.bind(columnDef))
        }
        Projection.COUNT_DISTINCT -> {
            val fields = projection.keys.flatMap { cp ->
                input.columns.filter { c -> cp.matches(c.name) }.ifEmpty { throw QueryException.QueryBindException("Column $cp could not be found in output.") }
            }.map {
                it.name to true
            }
            val columnDef = ColumnDef(Name.ColumnName(fields.first().first.schemaName, fields.first().first.entityName,"count(${fields.joinToString(",") { it.first.columnName }})"), Types.Long, false)
            CountProjectionLogicalOperatorNode(SelectDistinctProjectionLogicalOperatorNode(input, fields, context.catalogue.config), context.bindings.bind(columnDef))
        }
        Projection.EXISTS -> {
            val columnName = projection.keys.first()
            val columnDef = ColumnDef(Name.ColumnName(columnName.schemaName, columnName.entityName, "exists(${columnName.columnName})"), Types.Long, false)
            ExistsProjectionLogicalOperatorNode(input, context.bindings.bind(columnDef))
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
    private fun parseProjectionColumns(projection: CottontailGrpc.Projection, simplify: Boolean = false): Map<Name.ColumnName, Name> {
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
                CottontailGrpc.Expression.ExpCase.LITERAL -> TODO()
                CottontailGrpc.Expression.ExpCase.EXP_NOT_SET,
                null -> throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): Projection element at index $i is malformed.")
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