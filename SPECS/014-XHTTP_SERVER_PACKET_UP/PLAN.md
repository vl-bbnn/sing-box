# PLAN: 014 — XHTTP_SERVER_PACKET_UP

## Архитектура

1. Расширить существующий v2ray registry server-конструкторами, сохранив
   встроенный upstream switch как fallback.
2. Зарегистрировать XHTTP server из `transport/v2rayxhttp/register.go` только
   под `with_xhttp`.
3. Добавить `server.go` и `server_conn.go`: HTTP/1.1+h2/h2c listener поверх
   штатного TLS/Reality listener, session map и bounded ordered upload queue.
4. Packet-up GET создаёт один `net.Conn` для штатного transport handler;
   POST кладёт копию body в очередь соответствующей сессии.
5. Сначала доказать loopback client↔server, затем отдельный stage A/B порт,
   затем переключить только XHTTP nginx SNI mapping/profile manifest.

## Зона касания upstream

- `transport/v2ray/transport.go`: один `// lx:begin xhttp` lookup перед
  существующим server switch.
- Остальное — новые файлы либо существующие downstream XHTTP-файлы.
- `option/v2ray_xhttp.go` расширяется только серверными bounded defaults,
  если live-профилю понадобятся настраиваемые лимиты.

## Rollback

- Xray unit/config не удаляются до успешной iPhone cellular-приёмки.
- Nginx SNI mapping меняется атомарно и имеет сохранённую предыдущую версию.
- Rollback возвращает только XHTTP stage upstream на Xray; stable/WLT не
  участвуют.
