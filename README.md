# LightningEver Eclair Plugins

이 저장소는 **LightningEver LSP** 운영에 필요한 두 개의 Eclair plugin 을 포함합니다. 모두 ACINQ 공식 plugin SPI (`fr.acinq.eclair.Plugin`) 를 구현하며, `java -cp lib/* fr.acinq.eclair.Boot <plugin.jar>` 형태로 로드됩니다.

| Plugin | 역할 |
|---|---|
| **channel-funding** | 폰의 dual-funded channel open 요청을 가로채서 LSP 측 contribution (기본 50M sat) 으로 응답하는 funding interceptor |
| **fcm-push** | (LightningEver 신규) Firebase Cloud Messaging 으로 모바일 폰을 깨워 **BOLT12 offer 의 오프라인 수신** 을 가능하게 하는 wake-up push 플러그인 |

---

## 디렉터리 구조

```
.
├── channel-funding/       # 기존 plugin (BitEver 운영 시점부터)
│   ├── pom.xml
│   └── src/main/...
├── fcm-push/              # 신규 plugin (2026-05-21)
│   ├── pom.xml
│   ├── src/main/resources/reference.conf
│   └── src/main/scala/fr/acinq/eclair/plugins/fcmpush/
│       ├── FcmPushPlugin.scala
│       ├── FcmPushConfig.scala
│       ├── FcmTokenRegistry.scala
│       ├── FcmOAuth2.scala
│       ├── FcmSender.scala
│       └── FcmPushActor.scala
└── pom.xml                # parent (modules: channel-funding, fcm-push)
```

`historical-gossip / offline-commands / custom-offer` 디렉터리는 ACINQ 의 옛 0.13.0-SNAPSHOT pom 을 참조하는 별개 plugin 으로, **현 reactor 에서는 빌드하지 않습니다** (parent pom 의 `<modules>` 에서 제외). 필요하면 부모 pom 을 0.13.1 로 일괄 업데이트한 후 재포함.

---

## 빌드

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 두 plugin 모두 빌드 (parent reactor)
./mvnw clean package -Dmaven.test.skip=true

# 또는 하나만
./mvnw package -pl fcm-push        -am -Dmaven.test.skip=true
./mvnw package -pl channel-funding -am -Dmaven.test.skip=true
```

산출물:
- `channel-funding/target/channel-funding-plugin-0.13.1.jar`
- `fcm-push/target/fcm-push-plugin-0.13.1.jar`

eclair 가 의존하는 `fr.acinq.eclair:eclair-core_2.13:0.13.1` 가 로컬 maven repo 에 있어야 합니다 (`~/.m2/repository/`). 없으면 먼저 LightningEver eclair fork 의 `mvn install -pl eclair-core -am -DskipTests` 실행.

---

## 배포

```bash
# 운영 환경
cp channel-funding/target/channel-funding-plugin-0.13.1.jar /root/.eclair/plugins/
cp fcm-push/target/fcm-push-plugin-0.13.1.jar               /root/.eclair/plugins/

# Eclair 기동 시 두 jar 경로 모두 인자로 전달
cd /root/bitever-eclair-dist/eclair-node-0.13.1-93cc2ab
nohup java -cp "lib/*" fr.acinq.eclair.Boot \
  /root/.eclair/plugins/channel-funding-plugin-0.13.1.jar \
  /root/.eclair/plugins/fcm-push-plugin-0.13.1.jar \
  > /root/.eclair/eclair-stdout.log 2>&1 &
```

기동 후 로그에서 다음 두 줄 확인:
```
fr.acinq.eclair.Boot - loaded plugin ChannelFundingPlugin
fr.acinq.eclair.Boot - loaded plugin FcmPushPlugin
```

---

## fcm-push 설정

`eclair.conf` 와 별도로, `~/.eclair/fcm_push.conf` 파일로 plugin 설정을 override 할 수 있습니다. 기본값은 plugin jar 내부의 `reference.conf`:

```hocon
fcm-push {
  enabled              = true                                              # token tracking + push
  service-account-file = ${user.home}"/.eclair/fcm-service-account.json"   # 권한 600 권장
  project-id           = "lightningever"                                   # Firebase project_id
  fcm-endpoint         = "https://fcm.googleapis.com"
  access-token-refresh-margin-seconds = 300
  http-timeout-seconds = 10
  android-priority     = "high"
  android-ttl-seconds  = 7200
}
```

`service-account-file` 은 Firebase Console → 프로젝트 설정 → 서비스 계정 → "새 비공개 키 생성" 으로 받은 JSON 파일이어야 합니다. 권한 `600` 으로 보호.

---

## 동작 원리 (fcm-push 짧게)

1. Phoenix 폰이 LSP 와 연결되면 `FCMToken` (lightning message tag **35017**) 을 보냄.
2. LSP 의 `Peer.scala` 가 이를 파싱해서 `EventStream` 에 `FcmTokenRegistered(nodeId, token, platform)` publish.
3. plugin 의 `FcmPushActor` 가 받아서 `peer_nodeId → token` in-memory map 에 저장.
4. 누군가 폰에게 결제를 보내면 `MessageRelay` 가 (폰 offline 시) `PeerReadyNotifier` 를 호출.
5. `PeerReadyNotifier` 의 `waitForPeerConnected` 진입 시 `WakeUpPeerRequested(nodeId, reason)` 이벤트 publish.
6. plugin 이 그 이벤트를 받아 registry 에서 token 조회 → **FCM HTTP v1 API 로 push POST** (OAuth2 service-account JWT 인증).
7. 폰이 깨어나서 LSP 와 재연결 → `PeerReadyNotifier` 가 `PeerReady` 응답 → 결제 정상 진행.

자세한 흐름은 LightningEver 본 저장소의 `260521OFFBOLT12.md` 참조.

---

## 주의

- `fcm-push-plugin` 은 **outbound HTTPS 만** 사용합니다 (Google FCM v1 endpoint). 추가 포트 점유 없음.
- service-account JSON 의 권한은 **Google FCM 의 임의 메시지 발송** 입니다. git/이미지에 절대 commit 금지. 별도 secret/volume 으로 관리.
- 토큰 저장소는 현재 **in-memory only** 입니다. eclair 재시작 시 폰이 재연결할 때까지 push 무동작. 후속 작업으로 SQLite 영속화 권장.

---

## 라이선스

Apache License, Version 2.0

---

## 260521OFFBOLT12 — 검증 완료

이번 브랜치에서 `fcm-push-plugin` 이 다음 흐름으로 정상 동작함을 폰 검증으로 확인:

1. 폰 B 가 LSP 와 연결 → `FCMToken` (tag 35017) 송신 → LSP 의 `Peer.scala` 가 EventStream 으로 `FcmTokenRegistered` publish → 본 plugin 의 `FcmPushActor` 가 `peer_nodeId → token` 맵에 저장.
2. 폰 A 가 폰 B 의 BOLT12 offer 로 송금 → LSP `MessageRelay` 가 `invoice_request` onion 메시지를 폰 B 에게 forward 시도 → 폰 B 가 offline 이면 `PeerReadyNotifier` 가 `WakeUpPeerRequested` publish → 본 plugin 이 FCM HTTP v1 (OAuth2 service-account JWT 인증) 으로 push 발사.
3. 폰 B 가 깨어나 LSP 와 reconnect → invoice 발급 → 폰 A 로 응답 → HTLC 결제 진행 → 폰 B 가 잠금화면에서도 결제 수신.

자세한 가이드는 본 저장소의 상위 LightningEver 프로젝트 폴더의 `260521OFFBOLT12.md` 참조.
