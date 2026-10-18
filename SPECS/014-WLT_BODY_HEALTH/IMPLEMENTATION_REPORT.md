# Промежуточный отчёт

Кандидат изолирован на WLT branch от точного source tree установленного core.
Opt-in поле wlt_body_probe не распознаётся обычной сборкой. Публичный
конструктор валидирует настройки до запуска; group lifecycle выполняет GET
с исходным TLS runtime context и общим budget15s, сохраняет Close errors.
История body отделена от shared HEAD, ограничена configured members,
привязана к leaf/interface generation/time. Cold/fallback paths не допускают
непроверенный ordinary member. Interface change отменяет старый reader;
новые проверки ограничены concurrency2, rolling2/min/group, spacing5s.
Ротация не оставляет поздние members навсегда за двумя stalled paths.
TCP HTTP proof не заявляется доказательством качества UDP или полного приложения.

Сохранены исходные отрицательные результаты: потеря group TLS context,
причины caller deadline, порядок socket deadline/context timer, смена
вложенного leaf и холодная single-member URLTest-обёртка. Единственный
настроенный child теперь проверяется без ожидания weak HEAD; неоднозначные,
missing и cyclic choices не допускаются. Public Dial сохраняет саму обёртку.

r3432/r3436 дополнительно выявили queued dial после socket timeout до
срабатывания context timer. Абсолютный deadline теперь проверяется до
очереди, после получения slot и перед worker start. Исходный concurrency test
не менялся; r3438 сохраняет все прежние target identities и проходит normal,
race, vet, плюс по20 повторов этого теста normal/race. Новые cold-start
регрессии используют реальную legacy URLTest с одним и несколькими children.

Предыдущие пять полных core modes r3424 прошли на прежнем tree; исходный
r3418 external TLS-fragment EOF не отменяется. Актуальный source требует
новых full/CLI gates, source publication/recovery, native artifact/source
binding и physical comparison. Ничего не установлено, не опубликовано,
профили/servers/default не изменены. Обе платформы ещё не квалифицированы;
iPhone занят. Статус O: это промежуточный кандидат, не завершённая задача.
