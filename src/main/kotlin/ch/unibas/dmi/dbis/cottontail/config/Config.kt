package ch.unibas.dmi.dbis.cottontail.config

import kotlinx.serialization.Serializable

@Serializable
data class Config(val _root: String, val lockTimeout: Int) {



}