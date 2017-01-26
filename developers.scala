import org.mongodb.scala.bson.collection.mutable.Document

object Developers extends DBRecord {
  override def getCollectionName(): String = {
    "developers"
  }
  override def addRowAsDocumentToIserted(row: Seq[String]): Unit = {
    val names: String = row(header.indexOf("Developers")).replaceAll("\\s\\s+", " ").trim()
    if(!names.isEmpty)
      insertedRecords(names) = Document("name" -> names)
  }
}