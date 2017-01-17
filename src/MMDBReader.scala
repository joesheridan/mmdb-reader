
/**
  * Created by joe on 09/01/2017.
  */
package mmdb

import java.nio.file.{Files, Paths}

abstract class Direction
case class Right() extends Direction
case class Left() extends Direction

/**
  * Custom logger which can be enabled or disabled
  */
object logger {
  val debugEnabled = false
  def debug(x: Any) = { if (debugEnabled) println(x) }
}

object MMDBReader extends App {

  // read db file
  val byteArray = Files.readAllBytes(Paths.get("GeoLite2-Country.mmdb"))

  // create ip address
  //val ip = new IP(62,254,119,11)
  val ip = new IPAddress("178.20.86.0")
  val bs = new BinarySearch(byteArray)
  val md = new MetaExtractor()

  val d = for {
    meta <- md.getMetaData(byteArray)
    geoDataRecord <- bs.getGeoDataRecord(0, ip, 0, meta.nodeCount, 128)
    de = new DataExtractor(byteArray, meta.getSearchTreeSectionSize + 16)
    data <- de.getIsoCode(de.getGeoDataOffset(geoDataRecord))
  } yield (data)
  println("isocode:"+d)

}
