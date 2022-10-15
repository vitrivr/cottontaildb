import io.javalin.http.Context
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.ListEntities

object TestController {

    fun getList(context: Context) {

        /** using ClientConfig's client, sending ListSchemas message to cottontaildb*/
        val result: TupleIterator = ClientConfig.client.list(ListSchemas())
        val tree: MutableList<TreeNode> = mutableListOf()

        /** iterate through schemas*/
        result.forEach { itSchema ->

            /** first value of tuple is the name */
            /**val schemaName = itSchema.asString(0)*/
            val schemaName = itSchema[0].toString().replace("warren.","")

            /** subtree for entities in schema */
            val subtree: MutableList<TreeNode> = mutableListOf()
            /** using this ClientConfig's client, sending ListEntities message to cottontaildb*/
            val entities = ClientConfig.client.list(ListEntities(schemaName))
                entities.forEach { itEntity ->
                    val entityName = itEntity[0].toString().replace("$schemaName.","")
                    subtree.add(TreeNode(entityName, null))
                }
            tree.add(TreeNode(schemaName, subtree))
        }
        //create json from tree object (with jackson dependency)
        context.json(tree)
    }


}

