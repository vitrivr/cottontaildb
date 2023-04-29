package org.vitrivr.cottontail.ui.model.dbo

/**
 * A [Dbo] as returned by the Cottontail DB API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Dbo(val name: String, val fqn: String, val type: DboType) {
    companion object {
        fun schema(fqn: String) = Dbo(fqn.split('.').last(), fqn, DboType.SCHEMA)
        fun entity(fqn: String) = Dbo(fqn.split('.').last(), fqn, DboType.ENTITY)
        fun column(fqn: String) = Dbo(fqn.split('.').last(), fqn, DboType.COLUMN)
    }
}