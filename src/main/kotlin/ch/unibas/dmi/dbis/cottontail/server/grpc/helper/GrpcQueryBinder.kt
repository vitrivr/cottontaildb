package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.*

import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlanFactory
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc

import ch.unibas.dmi.dbis.cottontail.knn.metrics.Distance

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * This helper class parses and binds queries issued through the GRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The GRPC query components are bound to Cottontail DB [DBO] objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [ExecutionPlan] is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class GrpcQueryBinder(val factor: ExecutionPlanFactory, val catalogue: Catalogue) {
    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates an [ExecutionPlan].
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @return [ExecutionPlan]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBind(query: CottontailGrpc.Query): ExecutionPlan{
        if (!query.hasFrom()) throw QueryException.QuerySyntaxException("The query lacks a valid FROM-clause.")
        if (!query.hasProjection()) throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection).")
        return when {
            query.from.hasEntity() -> parseAndBindSimpleQuery(query)
            else -> throw QueryException.QuerySyntaxException("The query lacks a valid FROM-clause.")
        }
    }

    /**
     * Parses and binds a simple [CottontailGrpc.Query] without any joins and thereby generates an [ExecutionPlan].
     *
     * @param query The simple [CottontailGrpc.Query] object.
     */
    private fun parseAndBindSimpleQuery(query: CottontailGrpc.Query): ExecutionPlan {
        val entity = try {
            this.catalogue.getSchema(query.from.entity.schema.name).getEntity(query.from.entity.name)
        } catch (e: QueryException) {
            throw QueryException.QueryBindException("Failed to bind ${query.from.entity.fqn()}. Schema or entity does not exist!")
        }

        /* Create projection task. */
        val projectionClause = parseAndBindProjection(entity, query.projection)
        val knnClause = if (query.hasKnn()) parseAndBindKnnPredicate(entity, query.knn) else null
        val whereClause = if (query.hasWhere()) parseAndBindBooleanPredicate(entity, query.where) else null

        /* Transform to ExecutionPlan. */
        return factor.simpleExecutionPlan(entity, projectionClause, knnClause = knnClause, whereClause = whereClause)
    }

    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param entity The [Entity] from which fetch columns.
     * @param where The [CottontailGrpc.Where] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(entity: Entity, where: CottontailGrpc.Where): BooleanPredicate =  when(where.predicateCase) {
        CottontailGrpc.Where.PredicateCase.ATOMIC ->  parseAndBindAtomicBooleanPredicate(entity, where.atomic)
        CottontailGrpc.Where.PredicateCase.COMPOUND ->  parseAndBindCompoundBooleanPredicate(entity, where.compound)
        CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param entity The [Entity] from which fetch columns.
     * @param projection The [CottontailGrpc.Knn] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindCompoundBooleanPredicate(entity: Entity, compound: CottontailGrpc.CompoundBooleanPredicate): CompoundBooleanPredicate {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(entity, compound.aleft)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(entity, compound.cleft)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.LEFT_NOT_SET -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(entity, compound.aright)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(entity, compound.cright)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.RIGHT_NOT_SET -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        return try {
            CompoundBooleanPredicate(ConnectionOperator.valueOf(compound.op.name), left, right)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${compound.op.name}' is not a valid connection operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param entity The [Entity] from which fetch columns.
     * @param projection The [CottontailGrpc.Knn] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindAtomicBooleanPredicate(entity: Entity, atomic: CottontailGrpc.AtomicLiteralBooleanPredicate): AtomicBooleanPredicate<*> {
        val column = entity.columnForName(atomic.attribute) ?: throw QueryException.QueryBindException("Failed to bind column ${atomic.attribute}. Column does not exist on entity ${entity.fqn}!")
        val operator = try {
            ComparisonOperator.valueOf(atomic.op.name)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${atomic.op.name}' is not a valid comparison operator for a boolean predicate!")
        }

        return when (column.type) {
            is DoubleColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Double>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toDoubleValue() }.toTypedArray())
            is FloatColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Float>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toFloatValue() }.toTypedArray())
            is LongColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Long>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toLongValue() }.toTypedArray())
            is IntColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Int>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toIntValue() }.toTypedArray())
            is ShortColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Short>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toShortValue() }.toTypedArray())
            is ByteColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Byte>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toByteValue() }.toTypedArray())
            is BooleanColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<Boolean>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toBooleanValue() }.toTypedArray())
            is StringColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<String>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toStringValue() }.toTypedArray())
            is FloatArrayColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<FloatArray>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toFloatVectorValue() }.toTypedArray())
            is DoubleArrayColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<DoubleArray>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toDoubleVectorValue() }.toTypedArray())
            is LongArrayColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<LongArray>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toLongVectorValue() }.toTypedArray())
            is IntArrayColumnType -> AtomicBooleanPredicate(column = column as ColumnDef<IntArray>, operator = operator, not = atomic.not, values = atomic.dataList.map { it.toIntVectorValue() }.toTypedArray())
        }
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param entity The [Entity] from which fetch columns.
     * @param projection The [CottontailGrpc.Knn] object.
     *
     * @return The resulting [ExecutionTask].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnnPredicate(entity: Entity, knn: CottontailGrpc.Knn): KnnPredicate<*> {
        val column = entity.columnForName(knn.attribute) ?: throw QueryException.QueryBindException("Failed to bind column ${knn.attribute}. Column does not exist on entity ${entity.fqn}!")

        /* Extracts the query vector. */
        val query: Array<Number> = when (knn.query.vectorDataCase){
            CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR ->  knn.query.floatVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR ->  knn.query.doubleVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.INTVECTOR ->  knn.query.intVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> knn.query.longVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
            null -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
        }

        /* Extracts the query vector. */
        val weights: Array<Number>? = when (knn.weights.vectorDataCase) {
            CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR ->  knn.weights.floatVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> knn.weights.doubleVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> knn.weights.intVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> knn.weights.longVector.vectorList.toTypedArray()
            CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> null
            null -> null
        }

        /* Generate the predicate. */
        return try {
            KnnPredicate(column = column, k = knn.k, query = query, weights = weights,  distance = Distance.valueOf(knn.distance.name))
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("The '${knn.distance}' is not a valid distance function for a kNN predicate.")
        }
    }

    /**
     * Parses and binds the projection part of a GRPC [CottontailGrpc.Query]
     *
     * @param entity The [Entity] from which fetch columns.
     * @param projection The [CottontailGrpc.Projection] object.
     *
     * @return The resulting [Projection].
     */
    private fun parseAndBindProjection(entity: Entity, projection: CottontailGrpc.Projection): Projection = try {
        Projection(type = ProjectionType.valueOf(projection.op.name), columns = projection.attributesList.map { entity.columnForName(it) ?: throw QueryException.QueryBindException("Failed to bind column $it. Column does not exist on entity ${entity.fqn}")}.toTypedArray())
    } catch (e: java.lang.IllegalArgumentException) {
        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
    }
}