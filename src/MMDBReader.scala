
/**
  * Created by joe on 09/01/2017.
  */
package mmdb

import java.nio.file.{Files, Paths}

abstract class Direction
case class Right() extends Direction
case class Left() extends Direction

abstract class FieldType
case class PTR() extends FieldType
case class STR() extends FieldType
case class DBL() extends FieldType
case class BYT() extends FieldType
case class UINT16() extends FieldType
case class UINT32() extends FieldType
case class MAP() extends FieldType

case class ControlInfo(fieldType: FieldType, payload: Int)

object MMDBReader extends App {

  // read db file

  val byteArray = Files.readAllBytes(Paths.get("GeoLite2-Country.mmdb"))

  // create ip address
  //val ip = IP(62,254,119,11)
  val ip = new IP(178,20,86,35)

  ip.display

  val bs = new BinarySearch(byteArray)
  val de = new DataExtractor()
  val md = new MetaExtractor()

  md.getMetaData(byteArray) match {
    case Some(meta) => {
      println("record size:" + meta.recordSize)
      println(f"node count: - " + meta.nodeCount)
      println(f"search tree section size: " + meta.getSearchTreeSectionSize)

      val dataPtr = bs.getDataPtr(0, ip, 0, meta.nodeCount, 128)
      dataPtr match {
        case Some(ptr) => {
          println(f"data location: 0x$ptr%x")
          val geoDataOffset = de.getGeoDataOffset(ptr, meta.getSearchTreeSectionSize)
          val data = de.getGeoData(geoDataOffset, byteArray)
          println("got data:" + data)
        }
        case None => println("data for ip not found")
      }
    }
    case None => println("Error retrieving metadata")
  }

}
