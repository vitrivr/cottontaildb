package org.vitrivr.cottontail.ui.utilities

import io.javalin.json.JsonMapper
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import kotlinx.serialization.serializer
import org.vitrivr.cottontail.core.values.*
import java.io.InputStream
import java.lang.reflect.Type

/**
* A [JsonMapper] implementation for Javalin that uses Kotlinx Serialization framework.
*
* @author Ralph Gasser
* @version 1.0.0
*/
object KotlinxJsonMapper: JsonMapper {

    /** The [SerializersModule] used by this [KotlinxJsonMapper]. */
    private val projectModule = SerializersModule {
        polymorphic(PublicValue::class) {
            subclass(BooleanValue::class)
            subclass(ByteValue::class)
            subclass(ShortValue::class)
            subclass(IntValue::class)
            subclass(LongValue::class)
            subclass(FloatValue::class)
            subclass(DoubleValue::class)
            subclass(Complex32Value::class)
            subclass(Complex64Value::class)
            subclass(BooleanVectorValue::class)
            subclass(IntVectorValue::class)
            subclass(LongVectorValue::class)
            subclass(FloatVectorValue::class)
            subclass(DoubleVectorValue::class)
            subclass(Complex32VectorValue::class)
            subclass(Complex64VectorValue::class)
            subclass(StringValue::class)
            subclass(DateValue::class)
            subclass(ByteStringValue::class)
        }
    }

    /** The [Json] object to perform de-/serialization with.  */
    private val json = Json { serializersModule = projectModule }


    /**
     * Converts an object [Any] to a JSON string using Kotlinx serialization. Javalin uses this method for
     * io.javalin.http.Context.json(Object), as well as the CookieStore class, WebSockets messaging, and JavalinVue.
     *
     * @param obj The object [Any] to serialize.
     * @param type The target [Type]
     * @return JSON string representation.
     */
    override fun toJsonString(obj: Any, type: Type): String {
        return this.json.encodeToString(this.projectModule.serializer(type), obj)
    }

    /**
     * Converts a JSON [String] to an object representation using Kotlinx serialization framework.
     *
     * @param json The [String] to parse.
     * @param targetType The target [Type]
     * @return Object [T]
     */
    override fun <T : Any> fromJsonString(json: String, targetType: Type): T {
        val deserializer = this.projectModule.serializer(targetType)
        return this.json.decodeFromString(deserializer, json) as T
    }



    /**
     * Converts a JSON [InputStream] to an object representation using Kotlinx serialization framework.
     *
     * @param json The [InputStream] to parse.
     * @param targetType The target [Type]
     * @return Object [T]
     */
    override fun <T : Any> fromJsonStream(json: InputStream, targetType: Type): T {
        val deserializer = this.projectModule.serializer(targetType)
        return this.json.decodeFromStream(deserializer, json) as T
    }
}