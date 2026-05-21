/*
 * Copyright 2026 LightningEver
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fr.acinq.eclair.plugins.fcmpush

import grizzled.slf4j.Logging

import java.net.{HttpURLConnection, URI}
import java.nio.charset.StandardCharsets

sealed trait FcmSendResult
object FcmSendResult {
  case object Ok extends FcmSendResult
  case object InvalidToken extends FcmSendResult   // 404/UNREGISTERED — drop from registry
  case class TransientError(code: Int, body: String) extends FcmSendResult
  case class PermanentError(code: Int, body: String) extends FcmSendResult
}

/**
 * Minimal FCM HTTP v1 send client.
 *
 * Payload: data-only message with high priority — Phoenix listens for any push with the
 * key "reason" present, so we keep the body small (Phoenix decodes the keys it cares about).
 */
class FcmSender(config: FcmPushConfig, oauth: FcmOAuth2) extends Logging {

  private val sendUrl = s"${config.fcmEndpoint}/v1/projects/${config.projectId}/messages:send"
  private val httpTimeoutMs = config.httpTimeout.toMillis.toInt

  def sendPush(deviceToken: String, reason: String, extra: Map[String, String] = Map.empty): FcmSendResult = {
    val dataFields = ("\"reason\":" + jsonString(reason)) +: extra.toSeq.map { case (k, v) =>
      jsonString(k) + ":" + jsonString(v)
    }
    val dataObject = dataFields.mkString("{", ",", "}")

    val payload =
      s"""{"message":{"token":${jsonString(deviceToken)},"android":{"priority":"${config.androidPriority}","ttl":"${config.androidTtl.toSeconds}s"},"data":$dataObject}}"""

    val accessToken = oauth.accessToken()

    val conn = URI.create(sendUrl).toURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setConnectTimeout(httpTimeoutMs)
    conn.setReadTimeout(httpTimeoutMs)
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
    conn.setRequestProperty("Authorization", s"Bearer $accessToken")

    val os = conn.getOutputStream
    try os.write(payload.getBytes(StandardCharsets.UTF_8)) finally os.close()

    val code = conn.getResponseCode
    val stream = if (code >= 200 && code < 300) conn.getInputStream else conn.getErrorStream
    val body = if (stream != null) new String(stream.readAllBytes(), StandardCharsets.UTF_8) else ""

    code match {
      case c if c >= 200 && c < 300 =>
        logger.info(s"FCM push delivered ($reason) token=...${deviceToken.takeRight(8)} resp=${body.take(120)}")
        FcmSendResult.Ok
      case 404 =>
        logger.warn(s"FCM push UNREGISTERED — token will be dropped: ...${deviceToken.takeRight(8)} resp=${body.take(200)}")
        FcmSendResult.InvalidToken
      case 400 if body.contains("INVALID_ARGUMENT") && body.contains("registration") =>
        logger.warn(s"FCM push INVALID registration token: ...${deviceToken.takeRight(8)} resp=${body.take(200)}")
        FcmSendResult.InvalidToken
      case c if c == 401 || c == 403 =>
        logger.error(s"FCM push auth error ($c): $body")
        FcmSendResult.PermanentError(c, body)
      case c if c >= 500 || c == 429 =>
        logger.warn(s"FCM push transient error ($c): $body")
        FcmSendResult.TransientError(c, body)
      case c =>
        logger.error(s"FCM push unexpected error ($c): $body")
        FcmSendResult.PermanentError(c, body)
    }
  }

  /** Escape a string into a JSON literal (minimal — Phoenix tokens / reason codes are ASCII-safe). */
  private def jsonString(s: String): String = {
    val sb = new StringBuilder("\"")
    s.foreach {
      case '\\' => sb.append("\\\\")
      case '"'  => sb.append("\\\"")
      case '\b' => sb.append("\\b")
      case '\f' => sb.append("\\f")
      case '\n' => sb.append("\\n")
      case '\r' => sb.append("\\r")
      case '\t' => sb.append("\\t")
      case c if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
      case c => sb.append(c)
    }
    sb.append('"').toString()
  }
}
