package ch.unibas.dmi.dbis.cottontail.execution.tasks.knn

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef

object KnnTask {
    /** Definition for the distance column. */
    val DISTANCE_COL = ColumnDef.withAttributes("distance", "DOUBLE")
}