package process

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object EdgeProcessor {

  def createCommentHasTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[String]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Comment.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, "hasTag")
    }
  }

  def createForumHasMemberEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, String)]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Forum.id").toString.toLong, row.getAs[Any]("Person.id").toString.toLong,
          ("joinDate" -> row.getAs[Any]("joinDate").toString))
    }
  }

  def createForumHasTagTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[String]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Forum.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, "hasTag")
    }
  }

  def createPersonEmailEmailAddressEdgeRdd(rdd: RDD[Row]): RDD[Edge[String]] = {
    // TODO: ???
    null
  }

  def createPersonHasInterestTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[String]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, "hasInterest")
    }
  }

  def createPersonKnowsPersonEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, String)]] = {
    rdd.map {
      row =>
        Edge(row.get(0).toString.toLong, row.get(1).toString.toLong,
          ("creationDate", row.getAs[Any]("creationDate").toString))
    }
  }

  def createPersonLikesCommentEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, String)]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Comment.id").toString.toLong,
          ("creationDate", row.getAs[Any]("creationDate").toString))
    }
  }

  def createPersonLikesPostEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, String)]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Post.id").toString.toLong,
          ("creationDate", row.getAs[Any]("creationDate").toString))
    }
  }

  def createPersonSpeaksLanguageEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, String)]] = {
    // TODO: ???
    null
  }

  def createPersonStudyAtOrganisationEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, Long)]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Organisation.id").toString.toLong,
          ("classYear", row.getAs[Any]("classYear").toString.toLong))
    }
  }

  def createPersonWorkAtOrganisationEdgeRdd(rdd: RDD[Row]): RDD[Edge[(String, Long)]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Organisation.id").toString.toLong,
          ("workFrom", row.getAs[Any]("workFrom").toString.toLong))
    }
  }

  def createPostHasTagTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[String]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Post.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, "hasTag")
    }
  }

  def createTagHasTypeTagClassEdgeRdd(rdd: RDD[Row]): RDD[Edge[String]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Tag.id").toString.toLong, row.getAs[Any]("TagClass.id").toString.toLong, "hasTag")
    }
  }

}
