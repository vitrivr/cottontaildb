package org.vitrivr.cottontail.database.queries.planning.cost


/**
 * Represents a unit of [Cost]. Used to measure and compare operations in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class Cost constructor(val io: Float = 0.0f, val cpu: Float = 0.0f, val memory: Float = 0.0f) : Comparable<Cost> {

    companion object {
        val ZERO = Cost(0.0f, 0.0f, 0.0f)
        val INVALID = Cost(Float.NaN, Float.NaN, Float.NaN)
    }


    operator fun plus(other: Cost): Cost = Cost(this.io + other.io, this.cpu + other.cpu, this.memory + other.memory)
    operator fun minus(other: Cost): Cost = Cost(this.io - other.io, this.cpu - other.cpu, this.memory - other.memory)
    operator fun times(other: Cost): Cost = Cost(this.io * other.io, this.cpu * other.cpu, this.memory * other.memory)
    operator fun div(other: Cost): Cost = Cost(this.io / other.io, this.cpu / other.cpu, this.memory / other.memory)
    operator fun plus(other: Number): Cost = Cost(this.io + other.toFloat(), this.cpu + other.toFloat(), this.memory + other.toFloat())
    operator fun minus(other: Number): Cost = Cost(this.io - other.toFloat(), this.cpu - other.toFloat(), this.memory - other.toFloat())
    operator fun times(other: Number): Cost = Cost(this.io * other.toFloat(), this.cpu * other.toFloat(), this.memory * other.toFloat())
    operator fun div(other: Number): Cost = Cost(this.io / other.toFloat(), this.cpu / other.toFloat(), this.memory / other.toFloat())
    override fun compareTo(other: Cost): Int = (2.0f * (this.cpu - other.cpu) + 1.25f * (this.io - other.io) + (this.memory - other.memory)).toInt()
}