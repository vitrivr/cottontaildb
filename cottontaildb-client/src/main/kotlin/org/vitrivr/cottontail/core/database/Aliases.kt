package org.vitrivr.cottontail.core.database

/** The "Beginning of Cursor" [TupleId] constant. */
const val BOC: TupleId = -1L

/** Type alias for [TupleId]; a [TupleId] is a positive [Long] value (negative [TupleId]s are invalid).*/
typealias TupleId = Long

/** Type alias for [TransactionId]; a [TransactionId] is a positive [Long] value (negative [TransactionId]s are invalid).*/
typealias TransactionId = Long