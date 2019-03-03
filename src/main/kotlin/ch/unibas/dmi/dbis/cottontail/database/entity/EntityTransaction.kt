package ch.unibas.dmi.dbis.cottontail.database.entity

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.index.Index

import ch.unibas.dmi.dbis.cottontail.model.basics.Countable
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.basics.ParallelScanable
import ch.unibas.dmi.dbis.cottontail.model.basics.Scanable

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface EntityTransaction : Transaction, Filterable, Scanable, ParallelScanable, Countable {}