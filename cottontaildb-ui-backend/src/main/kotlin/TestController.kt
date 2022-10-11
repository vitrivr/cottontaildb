import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.javalin.http.Context
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.ListEntities

object TestController {

    //reused code from org.vitrivr.cottontail.cli
    val host: String = "localhost"
    val port: Int = 1865

    /** The [ManagedChannel] used to connect to Cottontail DB. */
    val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
        .enableFullStreamDecompression()
        .usePlaintext()
        .build()

    /** The [SimpleClient] used to access Cottontail DB. */
    val client = SimpleClient(channel)


    fun getList(context: Context) {

        val result: TupleIterator = client.list(ListSchemas())
        val tree: MutableList<TreeNode> = mutableListOf()


        result.forEach { itSchema ->

            val schemaName = itSchema[0].toString()

            println(schemaName)

            val subtree: MutableList<TreeNode> = mutableListOf()

            val entities = this.client.list(ListEntities(schemaName))
                entities.forEach { itEntity ->
                    val entityName = itEntity[0].toString().replace("$schemaName.","")
                    context.json(entityName)
                    subtree.add(TreeNode(entityName, null))
                }
            tree.add(TreeNode(schemaName, subtree))
        }
        context.json(tree)

    }


}

