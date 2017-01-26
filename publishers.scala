import org.mongodb.scala.bson.collection.mutable.Document

object Publishers extends DBRecord {
  override def getCollectionName(): String = {
    "publishers"
  }
  override def addRowAsDocumentToIserted(row: Seq[String]): Unit = {
    val names: String = row(header.indexOf("Publishers")).replaceAll("\\s\\s+", " ").trim()
    if(!names.isEmpty)
      insertedRecords(names) = Document("name" -> names)
  }
}