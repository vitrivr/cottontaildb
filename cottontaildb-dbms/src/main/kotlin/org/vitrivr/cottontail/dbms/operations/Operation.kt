package org.vitrivr.cottontail.dbms.operations

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.core.database.TransactionId
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [Operation] that can be executed by the Cottontail DB database engine.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Operation(val txId: TransactionId) {

    /** The [OperationType] of this [Operation]. */
    abstract val opType: OperationType

    /**
     * [Operation] that captures a COMMIT
     */
    class CommitOperation(txId: TransactionId): Operation(txId) {
        override val opType: OperationType = OperationType.COMMIT
    }

    /**
     * [Operation] that captures a ROLLBACK
     */
    class RollbackOperation(txId: TransactionId): Operation(txId) {
        override val opType: OperationType = OperationType.ROLLBACK
    }

    /**
     * [Operation] that captures the creation of a [Schema].
     */
    class CreateSchemaOperation(txId: TransactionId, val schema: Name.SchemaName): Operation(txId) {
        override val opType: OperationType = OperationType.CREATE_SCHEMA
    }

    /**
     * [Operation] that captures dropping of a [Schema].
     */
    class DropSchemaOperation(txId: TransactionId, val schema: Name.SchemaName): Operation(txId) {
        override val opType: OperationType = OperationType.DROP_SCHEMA
    }

    /**
     * [Operation] that captures creation of an [Entity].
     */
    class CreateEntityOperation(txId: TransactionId, val entity: Name.EntityName, val columns: List<ColumnDef<*>>): Operation(txId) {
        override val opType: OperationType = OperationType.CREATE_ENTITY
    }

    /**
     * [Operation] that captures dropping of an [Entity].
     */
    class DropEntityOperation(txId: TransactionId, val entity: Name.EntityName): Operation(txId) {
        override val opType: OperationType = OperationType.DROP_ENTITY
    }

    /**
     * [Operation] that captures creation of an [Index].
     */
    class CreateIndexOperation(txId: TransactionId, val index: Name.IndexName, val type: IndexType, val columns: Array<Name.ColumnName>, val params: Map<String, String> = emptyMap()): Operation(txId) {

        override val opType: OperationType = OperationType.CREATE_INDEX
    }

    /**
     * [Operation] that captures creation of an [Index].
     */
    class DropIndexOperation(txId: TransactionId, val index: Name.IndexName): Operation(txId) {
        override val opType: OperationType = OperationType.DROP_INDEX
    }

    sealed class DataManagementOperation(txId: TransactionId, val entity: Name.EntityName, val tupleId: TupleId): Operation(txId) {
        /**
         * A [DataManagementOperation] that signals an INSERT into an [Entity]
         */
        class InsertOperation(txId: TransactionId, entity: Name.EntityName, tupleId: TupleId, val inserts: Map<ColumnDef<*>, Value?>) : DataManagementOperation(txId, entity, tupleId) {
            override val opType: OperationType = OperationType.INSERT
        }

        /**
         * A [DataManagementOperation] that signals an UPDATE in an [Entity]
         */
        class UpdateOperation(txId: TransactionId, entity: Name.EntityName, tupleId: TupleId, val updates: Map<ColumnDef<*>, Pair<Value?, Value?>>, ) : DataManagementOperation(txId, entity, tupleId) {
            override val opType: OperationType = OperationType.UPDATE
        }

        /**
         * A [DataManagementOperation] that signals a DELETE from an [Entity]
         */
        class DeleteOperation(txId: TransactionId, entity: Name.EntityName, tupleId: TupleId, val deleted: Map<ColumnDef<*>, Value?>) : DataManagementOperation(txId, entity, tupleId) {
            override val opType: OperationType = OperationType.DELETE
        }
    }
}