# SPEC: 014 — XHTTP_SERVER_PACKET_UP

| Поле | Значение |
|------|----------|
| Тип | F (feature) |
| Статус | O (open) |

## 1. Контекст

Форк уже реализует XHTTP client/outbound и успешно работает с Xray 25.6.8.
Изолированный stage-контур подтвердил iPhone → sing-box-lx client → Xray
packet-up, но владелец проекта уточнил конечную архитектуру: обе стороны
должны использовать модифицированный sing-box, а Xray не должен оставаться в
активном пути.

Upstream sing-box не поддерживает XHTTP и не планирует поддержку. Текущий
server dispatcher намеренно не знает `transport.type=xhttp`.

## 2. Цель

VLESS inbound с Reality и `transport.type=xhttp`, `mode=packet-up` принимает
существующий sing-box-lx XHTTP client wire protocol и передаёт двусторонний TCP
поток в штатный VLESS service. Реализация должна позволить заменить Xray только
в XHTTP stage-контуре без изменений WLT, stable и протокольного VLESS inbound.

## 3. Требования

- Server constructor регистрируется за `with_xhttp`; без тега поведение
  upstream не меняется.
- Поддерживается Xray-compatible packet-up layout:
  `GET <path>/<session-id>` для downlink и
  `POST <path>/<session-id>/<seq>` для uplink.
- Uplink-пакеты собираются строго по `seq`, имеют ограниченный размер и
  ограниченную очередь; незавершённые сессии удаляются по TTL.
- Проверяются Host, path и default Xray padding в query параметре
  `x_padding` заголовка `Referer`.
- Ответ отключает buffering/cache и flush'ит каждый downlink write.
- TLS/Reality использует штатный `tls.ServerConfig`; VLESS/VMess/Trojan не
  получают отдельных правок.
- Режимы `stream-up`, `stream-one`, HTTP/3 и расширенные placement/xmux не
  маскируются под поддержанные: конфиг отвергается явно.

## 4. Критерии приёмки

- `go build ./...` без `with_xhttp` проходит и xhttp inbound отвергается.
- Сборка с lx-тегами проходит; `sing-box check` принимает VLESS+Reality+XHTTP
  packet-up inbound.
- Unit/integration tests покрывают path/padding, упорядочивание packet-up,
  bounded queue, close/TTL и двусторонний HTTP/2 поток.
- Живой stage probe скачивает не менее 128 KiB через sing-box client ↔
  sing-box server.
- Физический iPhone обновляет существующий профиль, поднимает туннель и
  открывает маршрутизируемые сайты по Wi-Fi и cellular.
- После live-приёмки Xray исключён из активного XHTTP stage-пути; stable и WLT
  ревизии/сервисы не изменены.

## 5. Вне скоупа

- Полный паритет Xray XHTTP extras, xmux, browser dialer и HTTP/3.
- Продвижение server-side XHTTP в stable до отдельного решения.
- Изменение WLT carrier или WLT профилей.
