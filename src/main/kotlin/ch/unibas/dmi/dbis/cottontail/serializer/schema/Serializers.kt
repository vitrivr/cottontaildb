package ch.unibas.dmi.dbis.cottontail.serializer.schema

object Serializers {

    /** Default [ColumnDefinitionSerializer] implementation.  */
    val COLUMN_DEF_SERIALIZER = ColumnDefinitionSerializer()

    /** Default [EntityDefinitionSerializer] implementation.  */
    val ENTITY_DEF_SERIALIZER = EntityDefinitionSerializer()
}
