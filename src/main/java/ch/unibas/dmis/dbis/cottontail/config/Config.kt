package ch.unibas.dmis.dbis.cottontail.config

import kotlinx.serialization.Serializable

@Serializable
data class Config(val dataFolder: String, val lockTimeout: Int)