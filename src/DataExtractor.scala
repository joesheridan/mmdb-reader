/**
  * Created by joe on 14/01/2017.
  */

package mmdb

class DataExtractor {

  def getGeoData(ptr: Int, bytes: Array[Byte]) = {
    val ci = readControlByte(ptr+1, bytes)
    val fieldtype = ci.fieldType
    val payload = ci.payload
    println("data found second cb: "+ readControlByte(ptr, bytes))
    println("data found second cb: "+ readControlByte(ptr+1, bytes))
    println("data found second cb: "+ readControlByte(ptr+3, bytes))
    println("data found second cb: "+ readControlByte(ptr+3, bytes))
    println("data found second cb: "+ readControlByte(ptr+4, bytes))
    println("data found second cb: "+ readControlByte(ptr+5, bytes))
  }

  def readControlByte(ptr: Int, bytes: Array[Byte]): ControlInfo = {
    val cbyte = bytes(ptr) & 0xFF
    val ctype = cbyte >> 5
    val payload = bytes(ptr) & 0x1f
    println("field type (first 3 bits) to int:" + ctype + " size in bytes:" + payload)
    val fieldtype = ctype match {
      case 1 => PTR()
      case 2 => STR()
      case 3 => DBL()
      case 4 => BYT()
      case 5 => UINT16()
      case 6 => UINT32()
      case 7 => MAP()
    }
    ControlInfo(fieldtype, payload)
  }

  def getPointerSize(ptr: Int, bytes: Array[Byte]) = {

  }
  def getGeoDataOffset(ptr: Int, sTreeSize: Int): Int = {
    ptr + sTreeSize + 16
  }
}
