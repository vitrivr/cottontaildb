package entity

import org.vitrivr.cottontail.client.iterators.Tuple

@Suppress("unused")
class EntityDetails (details: Tuple){
    var dbo: String? = details.asString("dbo")
    var _class : String? = details.asString("class")
    var type : String? = details.asString("type")
    var rows: Int? = details.asInt("rows")
    var lsize : Int? = details.asInt("l_size")
    var nullable : Boolean? = details.asBoolean("nullable")
    val info : String? = details.asString("info")
}
