/**
  * Created by joe on 14/01/2017.
  */
package mmdb

import scala.util.{Try, Success, Failure}

case class IP(ip1: Int, ip2: Int, ip3: Int, ip4: Int)

/**
  * Creates a representation of an IP address and allows operations on the underlying
  * octet and integer values
  * @param ipStr
  */
class IPAddress(ipStr: String) {

  // try to parse the given ip string
  private val ip = parseIPStr(ipStr).getOrElse {
    throw new Exception("Incorrect IP format")
  }

  /**
    * Parse the ipStr param into a valid IP address with 4 octets
    * @param ipStr
    * @return - an IP case class
    */
  def parseIPStr(ipStr: String): Try[IP] = {
    Try {
      val ipStrings = ipStr.split('.')
      if (ipStrings.length < 4) {
        throw new Exception()
      }
      val octets = ipStrings.map { x => Integer.parseInt(x) }

      if (octets.max > 255 || octets.min < 0) {
        throw new Exception()
      }

      IP(octets(0), octets(1), octets(2), octets(3))
    }
  }

  /**
    * shift the 4 ip bytes into place and return as one Int
    * @return - the integer representation of the given ip numbers
    */
  def getInt(ip: IP): Int = ((ip.ip1 << 24) + (ip.ip2 << 16) + (ip.ip3 << 8) + ip.ip4)

  /**
    * Gets a binary string version of the IP - padded with length param zeros
    * @param length
    * @return
    */
  def getString(length: Int): String = {
    val bs = getInt(ip).toBinaryString
    padTo(length, bs)
  }

  /**
    * Gets the v6 mapped binary representation of the IPv4 address
    * see here: https://en.wikipedia.org/wiki/IPv6#IPv4-mapped_IPv6_addresses
    * @return - the 128 character binary string of the mapped v6-v4 ip address
    */
  def getv6Mapped: String = {
    val zeros = List.fill(80)("0").mkString
    val ones = List.fill(16)("1").mkString
    zeros + ones + getString(32)
  }

  /**
    * Displays the ip address for debugging purposes
    */
  def display = {
    val b = getInt(ip)
    logger.debug(f"IP display, hex value: $b%x")
    logger.debug("bit length: " + getString(32).length)
    logger.debug("binary: " + getString(32))
    logger.debug("ipv6: " + getv6Mapped)
  }

  /**
    * Pads a string with leading zeros
    * @param length - the final length of the string
    * @param str - the input string
    * @return - a string with leading zeros of length length
    */
  def padTo(length: Int, str: String) : String = {
    val zeros = length - str.length
    val zerosStr = List.fill(zeros)("0")
    zerosStr.mkString + str
  }

}
