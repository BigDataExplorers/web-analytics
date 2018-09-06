package com.webanalytics.preprocess

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }

object IPEncryptor {

  private val Algorithm = "AES/CBC/PKCS5Padding"
  private val Key = new SecretKeySpec(Base64.getDecoder.decode("DxVnlUlQSu3E5acRu7HPwg=="), "AES")
  private val IvSpec = new IvParameterSpec(new Array[Byte](16))
  private val cipher = Cipher.getInstance(Algorithm)
  cipher.init(Cipher.ENCRYPT_MODE, Key, IvSpec)
  
  def encrypt(text: String): String = {
    new String(Base64.getEncoder.encode(cipher.doFinal(text.getBytes("utf-8"))), "utf-8")
  }

}