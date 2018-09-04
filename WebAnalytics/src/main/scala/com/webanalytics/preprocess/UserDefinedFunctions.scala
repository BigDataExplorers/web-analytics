package com.webanalytics.preprocess
import org.apache.spark.sql.functions.udf
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }

object UserDefinedFunctions{
  
  //Required for IP Encryption
  private val Algorithm = "AES/CBC/PKCS5Padding"
  private val Key = new SecretKeySpec(Base64.getDecoder.decode("DxVnlUlQSu3E5acRu7HPwg=="), "AES")
  private val IvSpec = new IvParameterSpec(new Array[Byte](16))
  private val cipher = Cipher.getInstance(Algorithm)
  cipher.init(Cipher.ENCRYPT_MODE, Key, IvSpec)
  
  /**
   * UDF to encrypt the IP Address
   * @param ipAddress the ip address to be encrypted
   * @return encrypted ip
   */
  def encryptUrl = udf((ipAddress: String) =>
    {
      new String(Base64.getEncoder.encode(cipher.doFinal(ipAddress.getBytes("utf-8"))), "utf-8")
    })
  
  /**
   * UDF to filter the request URL from the request body
   * @param rawData the request URL
   * @return the resource part of the URL
   */
  def getUrl = udf((rawData: String) =>
    {
      rawData.split(" ")(1)
    })
    
  /**
   * UDF to get the integer representation of the host IP
   * IF the host is 1.2.3.4 => (1 * 256^3) + (2 * 256^2) + (3 * 256) + 4
   * @param ipString the ip which consists of numbers separated by '.'
   * @return the integer value of host IP
   */
  def ipToIntUDF = udf { ipString: String =>
    ((ipString.split('.')(0)).toLong * 16777216) + ((ipString.split('.')(1)).toLong * 65536) +
      ((ipString.split('.')(2)).toLong * 256) + (ipString.split('.')(3)).toLong
  }

  /**
   * UDF to get the last characters from the host name
   * @param ipString the host name
   * @return the last characters from host name
   */
  def hostToCountry = udf { ipString: String => ipString.split('.').last }
}