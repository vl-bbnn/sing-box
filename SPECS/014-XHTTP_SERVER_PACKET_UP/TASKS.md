# TASKS: 014 — XHTTP_SERVER_PACKET_UP

- [x] Зафиксировать пользовательское архитектурное требование и scope.
- [x] Подтвердить, что текущий форк реализует только XHTTP client.
- [ ] Добавить server registry hook за `with_xhttp`.
- [ ] Реализовать bounded ordered packet-up session/conn.
- [ ] Реализовать HTTP/2 server transport, валидацию и cleanup.
- [ ] Добавить unit/integration tests и inbound check-config.
- [ ] Пройти DoD без тегов и с lx-тегами.
- [ ] Собрать и развернуть отдельный sing-box XHTTP stage A/B.
- [ ] Пройти 128 KiB probe client↔server.
- [ ] Переключить существующий iPhone stage-профиль на sing-box server.
- [ ] Подтвердить Wi-Fi и cellular на физическом iPhone.
- [ ] Удалить Xray из активного пути, сохранить rollback и заполнить REPORT.
