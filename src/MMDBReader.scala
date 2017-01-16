
/**
  * Created by joe on 09/01/2017.
  */
package mmdb

import java.nio.file.{Files, Paths}

abstract class Direction
case class Right() extends Direction
case class Left() extends Direction


object MMDBReader extends App {

  // read db file

  val byteArray = Files.readAllBytes(Paths.get("GeoLite2-Country.mmdb"))

  // create ip address
  //val ip = new IP(62,254,119,11)
  val ip = new IP(178,20,86,35)

  ip.display

  val bs = new BinarySearch(byteArray)
  val md = new MetaExtractor()

  /*for {
    meta <- md.getMetaData(byteArray)
    dataPtr <- bs.getDataPtr(0, ip, 0, meta.nodeCount, 128)
    de = new DataExtractor(byteArray, meta.getSearchTreeSectionSize + 16)
    data <- de.getIsoCode(de.getGeoDataOffset(dataPtr))
  } yield (data)
*/

  md.getMetaData(byteArray) match {
    case Some(meta) => {
      println("record size:" + meta.recordSize)
      println(f"node count: - " + meta.nodeCount)
      println(f"search tree section size: " + meta.getSearchTreeSectionSize)

      val de = new DataExtractor(byteArray, meta.getSearchTreeSectionSize + 16)

      val dataPtr = bs.getDataPtr(0, ip, 0, meta.nodeCount, 128)
      dataPtr match {
        case Some(ptr) => {
          println(f"data location: 0x$ptr%x")
          val geoDataOffset = de.getGeoDataOffset(ptr)
          val data = de.getIsoCode(geoDataOffset)
          println("got data:" + data)
        }
        case None => println("data for ip not found")
      }
    }
    case None => println("Error retrieving metadata")
  }

}
