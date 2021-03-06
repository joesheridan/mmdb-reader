
package mmdb

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
  * Custom logger which can be enabled or disabled
  */
object logger {
  val debugEnabled = false
  def debug(x: Any) = { if (debugEnabled) println(x) }
}

/**
  * Provides high level functions to extract geo data from a given mmdb database file
  */
object MMDBReader {

  def main(args: Array[String]) = {
    println(lookupIP("GeoLite2-Country.mmdb", "178.20.86.35"))
  }

  /**
    * Retrieves geo-location information about a given IP
    * @param dbfile - the mmdb file to read
    * @param ipStr - a string containing the IP address
    * @return - the iso country code string corresponding to the IP location
    */
  def lookupIP(dbfile: String, ipStr: String): Try[String] = {

    // read db file
    val byteArray = Files.readAllBytes(Paths.get(dbfile))

    // create ip address
    val ip = new IPAddress(ipStr)
    val bs = new BinarySearch(byteArray)
    val md = new MetaExtractor()

    for {
      meta <- md.getMetaData(byteArray)
      geoDataRecord <- bs.getGeoDataRecord(0, ip, 0, meta.nodeCount, 128)
      de = new DataExtractor(byteArray, meta.getSearchTreeSectionSize + 16)
      data <- de.getIsoCode(de.getGeoDataOffset(geoDataRecord))
    } yield (data)

  }


}
