package test

import model.{CommentProperty, TagProperty, VertexProperty}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import process.{EdgeProcessor, VertexProcessor}

sealed trait EdgeProperty

case class AuthorEdgeProperty(val doccount: Long) extends EdgeProperty

case class CiteEdgeProperty() extends EdgeProperty

object ProcessGraph {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkQueries")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val SCHEMA_OPTIONS = Map("header" -> "true", "inferSchema" -> "true", "sep" -> "|")

    val commentVerticesPath = "src/main/resources/comment_0_0.csv"
    val forumVerticesPath = "src/main/resources/forum_0_0.csv"
    val organisationVerticesPath = "src/main/resources/organisation_0_0.csv"
    val personVerticesPath = "src/main/resources/person_0_0.csv"
    val placeVerticesPath = "src/main/resources/place_0_0.csv"
    val postVerticesPath = "src/main/resources/post_0_0.csv"
    val tagVerticesPath = "src/main/resources/tag_0_0.csv"
    val tagClassVerticesPath = "src/main/resources/tagclass_0_0.csv"


    val commentHasTagTagEdgesPath = "src/main/resources/comment_hasTag_tag_0_0.csv"
    val forumHasMemberPersonEdgesPath = "src/main/forum_hasMember_person_0_0.csv"
    val forumHasTagTagEdgesPath = "src/main/forum_hasTag_tag_0_0.csv"
    val personHasInterestTagEdgesPath = "src/main/person_hasInterest_tag_0_0.csv"
    val personKnowsPersonEdgesPath = "src/main/person_knows_person_0_0.csv"
    val personLikesCommentEdgesPath = "src/main/person_likes_comment_0_0.csv"
    val personLikesPostEdgesPath = "src/main/person_likes_comment_0_0.csv"
    val personStudyAtOrganisationEdgesPath = "src/main/person_studyAt_organisation_0_0.csv"
    val personWorkAtOrganisationEdgesPath = "src/main/person_workAt_organisation_0_0.csv"
    val postHasTagTagEdgesPath = "src/main/post_hasTag_tag_0_0.csv"
    val tagHasTypeTagClassEdgesPath = "src/main/tag_hasType_tagclass_0_0.csv"

    val commentInitialRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentVerticesPath).rdd
    val tagInitialRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(tagVerticesPath).rdd
    val commentTagEdgesInitialRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentTagEdgesPath).rdd
    spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentVerticesPath).rdd

    val commentRdd = VertexProcessor.createCommentVertexRdd(commentInitialRdd)
    val tagRdd = VertexProcessor.createTagVertexRdd(tagInitialRdd)


    val unifiedVertices: RDD[(VertexId, VertexProperty)] = commentRdd.union(tagRdd)

    //commentTagEdgesInitialRdd.take(10).foreach(println)

    val commentTagEdgeRdd = EdgeProcessor.createCommentHasTagEdgeRdd(commentTagEdgesInitialRdd)

    val graph = Graph(unifiedVertices, commentTagEdgeRdd)
    graph.edges.take(10).foreach(println)

  }


}
