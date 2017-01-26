import org.mongodb.scala.bson.collection.mutable.Document

object Genres extends DBRecord {
  override def getCollectionName(): String = {
    "genres"
  }
  override def addRowAsDocumentToIserted(row: Seq[String]): Unit = {
    val names: String = row(header.indexOf("Genres")).replaceAll("\\s\\s+", " ").trim()
    if(!names.isEmpty)
      for (name <- names.split(","))
        insertedRecords(name) = Document("name" -> name)
  }
}