package org.vitrivr.cottontail.ui.json

import io.javalin.json.JsonMapper
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import java.lang.reflect.Type

/**
 * A [JsonMapper] implementation for Javalin based on kotlinx.serialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@OptIn(ExperimentalSerializationApi::class)
object KotlinxJsonMapper: JsonMapper {
    private val json = Json { encodeDefaults = true }
    override fun toJsonString(obj: Any, type: Type): String {
        val serializer = serializer(obj.javaClass)
        return json.encodeToString(serializer, obj)
    }

    override fun <T : Any> fromJsonString(json: String, targetType: Type): T {
        @Suppress("UNCHECKED_CAST")
        val deserializer = serializer(targetType) as KSerializer<T>
        return this.json.decodeFromString(deserializer, json)
    }
}