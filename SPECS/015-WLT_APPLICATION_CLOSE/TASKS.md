# Задачи

- [x] Изолированный helper и минимальный клиентский шов.
- [x] Тесты выбора API, legacy/Abort, raw errors, одного владельца.
- [x] Normal/race и прежние target tests/vet/default gates (r4090).
- [x] Carrier full gates r4088, recovery r4089/r4092 и published pin r4093.
- [ ] Native source integration и сборка.
- [ ] Новый физический прогон и проверка исходного Close/Stop/media.

- [x] Scoped opt-in core selection, service/JSON validation и host gates (385 normal/race, 32 default).
- [ ] Resolved integration/branch contracts, recovery и native artifacts.
- [ ] Fresh Pixel field validation; iPhone только после нового возврата.

- [x] Host воспроизведение подмены request deadline и первой Read ошибки.
- [x] Детерминированное воспроизведение retained EOF interleaving и source fix.
- [x] Focused normal/race, реальные повторные VLESS/KCP сценарии и negatives.
- [ ] Полные host regression gates и immutable dependency/source binding.
- [ ] Подписанные native artifacts и свежая physical/media квалификация.

- [x] First-prior field audit: 17 доказанных неизменённых внутренних 30s idle read caps.
- [x] KCP pending-I/O deadline bug: 232 normal/race, 1/6 GiB, два negative mutants.
- [x] Same-KCP idle-owner baseline/candidate normal/race, сохранён unknown initial caller cap.
- [ ] Immutable core feature regression/source recovery, merge-only native integration.
- [ ] Новая physical/media приёмка; прежние 28 Close и 6 EPIPE не считать исправленными.
