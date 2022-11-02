package schema

import ClientConfig
import com.google.gson.Gson
import io.javalin.http.Context
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.grpc.CottontailGrpc

object SchemaController {

    private val gson = Gson()

    fun createSchema(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val schema: Schema = gson.fromJson(context.body(), Schema::class.java)
        val result : TupleIterator = clientConfig.client.create(CreateSchema(schema.name))
        context.json(result)

    }
    fun dropSchema(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val schemaName = context.pathParam("name")
        val result : TupleIterator = clientConfig.client.drop(DropSchema(schemaName))
        context.json(result)
    }
    fun dumpSchema(context: Context){
        TODO()
    }

    fun listAllSchemas(context: Context) {
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        /** using ClientConfig's client, sending ListSchemas message to cottontaildb*/
        val result: TupleIterator = clientConfig.client.list(ListSchemas())
        val schemas: MutableList<Schema> = mutableListOf()
        /** iterate through schemas*/
        result.forEach {
            if (it.asString(0).isNullOrBlank()){
                TODO()
            } else {
                //it.asString(0) is nullable
                schemas.add(Schema(it[0].toString()))
            }
            /** first value of tuple is the name */
            /**val schemaName = itSchema.asString(0)*/
        }
        context.json(schemas)
    }

    fun listEntities(context: Context){
        val clientConfig = ClientConfig(context.pathParam("port").toInt())
        val schemaName = context.pathParam("name")

        val result: TupleIterator = clientConfig.client
            .list(
                CottontailGrpc.ListEntityMessage.newBuilder().setSchema(
                    CottontailGrpc.SchemaName.newBuilder()
                        .setName(schemaName).build()).build())

        val entities: MutableList<String?> = mutableListOf()
        /** iterate through entities*/
        result.forEach { it ->
            entities.add(it.asString(0))
        }
        context.json(entities)
    }


}