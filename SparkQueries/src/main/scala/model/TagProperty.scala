package model

case class TagProperty(
                        val url: Option[String],
                        val hasType: Option[Long]
                      ) extends VertexProperty
