import com.google.gson.Gson
import io.javalin.http.Context
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.grpc.CottontailGrpc


object SchemaController {

    fun createSchema(context: Context){
        val gson = Gson()
        val schema: Schema = gson.fromJson(context.body(), Schema::class.java)
        val result : TupleIterator = ClientConfig.client.create(CreateSchema(schema.name))
        context.json(result)

    }
    fun dropSchema(context: Context){
        val schemaName = context.pathParam("name")
        val result : TupleIterator = ClientConfig.client.drop(DropSchema(schemaName))
        println("hello")
        context.json(result)
    }
    fun dumpSchema(context: Context){
        TODO()
    }

    fun listAllSchemas(context: Context) {
        /** using ClientConfig's client, sending ListSchemas message to cottontaildb*/
        val result: TupleIterator = ClientConfig.client.list(ListSchemas())
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

        val schemaName = context.pathParam("name")

        val result: TupleIterator = ClientConfig.client
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