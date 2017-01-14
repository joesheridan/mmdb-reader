/**
  * Created by joe on 14/01/2017.
  */
package mmdb

class IP(ip1:Int, ip2:Int, ip3:Int, ip4:Int) {

  /**
    * shift the 4 ip bytes into place and return as one Int
    * @return - the integer representation of the given ip numbers
    */
  def getInt: Int = {
    ((ip1 << 24) + (ip2 << 16) + (ip3 << 8) + ip4)
  }

  def getString(length:Int): String = {
    val bs = getInt.toBinaryString
    val zerosNeeded = length - bs.length
    val zeros = List.fill(zerosNeeded)("0").mkString
    zeros + bs
  }

  def getv6Mapped: String = {
    val zeros = List.fill(80)("0").mkString
    val ones = List.fill(16)("0").mkString
    zeros + ones + getString(32)
  }


  def display = {
    val b = getInt
    println(f"IP display, hex value: $b%x")
    println("bit length: " + getString(32).length)
    println("binary: " + getString(32))
    println("ipv6: " + getv6Mapped)
  }

  def padTo(length: Int, str: String) : String = {
    val zeros = length - str.length
    val zerosStr = List.fill(zeros)("0")
    zerosStr.mkString + str
  }

}
