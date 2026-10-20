# WLT body health для автоматического merged выбора

| Поле | Значение |
|---|---|
| Тип | F |
| Статус | O |

Источник задачи — действующее пользовательское поручение стабилизировать WLT.
Это изолированное расширение WLT-ветки; обычные overlay/LX остаются без WLT.
HEAD может успешно завершаться на пути, где GET обрывается. При холодной истории
первый ordinary member раньше мог выбираться до проверки.

Требования: opt-in wlt_body_probe только with_wlt, explicit HTTPS URL, exact
1..65536 bytes + EOF/status200/strictTLS, исходный общий15s включая Close.
Все прежние HEAD tests и defaults без изменений. Никаких transparent replay.
Отдельная bounded история группы, scoped по leaf/generation/time. Ни shared
HEAD, ни UI/другая группа, ни connect success не создают body proof.
До body proof обычного маршрута — только явно заданный unverified_fallback
(для WLT profile это WLT), без собственного сервера как условия bootstrap.
Все TCP/UDP/direct/status/selection/fallback paths соблюдают политику.
Не более8members,2параллельных probe,65537 application body bytes на пробу плюс bounded HTTP/TLS buffering, interval>=1m,
один15s batch budget; steady interval>=1m; interface может использовать bounded extra check,
но force/UI/interface не обходят rolling cap2batches/min/group и spacing5s.
На interface change прежние proofs перестают действовать немедленно; если
traffic admission (включая cap2/min) пока закрыт, используется только explicit fallback до
следующей разрешённой проверки, а не старое ordinary health.
Остановка присоединяет активную проверку; результаты старого интерфейса/leaf
не публикуются. Нужны regressions HEAD-ok/GET-stall/truncate, реальные TLS trust,
Close errors, cancel/deadline, no replay, history/selection/interface isolation,
bounded admission/traffic и defaults. Host gates не означают physical acceptance.

Дальше: full original core gates, independent review/recovery, native artifact
and in-place install, physical comparison with unchanged HTTP/Close gates.
Без проверки не менять профили API, shared services или default contour.
