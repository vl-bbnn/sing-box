# WLT_APPLICATION_CLOSE

| Поле | Значение |
|------|----------|
| Тип | B (bug) |
| Статус | O (open) |

При полном закрытии клиентского потока непрочитанный ответ может заполнить
приёмную очередь и удерживать DATA Close до исходного deadline. Ядро должно
вызывать optional CloseApplication у поддерживающего carrier. Обычный Close
старого транспорта и немедленный Abort сохраняют прежнее поведение.

Изоляция: только with_wlt; серверный retained reverse-read Close не меняется.
Никаких новых ACK, протокола, зависимостей или увеличения close deadlines.
Исходные ошибки не фильтровать. Один владелец Close и одно освобождение слота.

Приёмка: normal/race и старые lifecycle tests; byte-exact обычные сборки по
source scope; опубликованный carrier pin, native сборка и новый физический
прогон с актуальным state. Host pass не закрывает физическую приёмку.

Дополнение scoped read retirement: отдельный opt-in `scoped_application_read_retirement`
(по умолчанию false, требует data_half_close). До публикации DATA активировать
carrier capability; отказ или исходный dial deadline завершают исходное владение
и освобождают допуск. Legacy CloseApplication и immediate Abort не меняются.
Полная physical/media приёмка остаётся открытой.

Диагностика первой исходной ошибки Read сохраняет её идентичность и ограниченные
снимки дедлайна/источника setter; конкурентные setter помечаются неопределёнными.
В scoped режиме Read deadline принадлежит только caller; idle activity его не
создаёт, не очищает и не продлевает. Idle закрывает существующий idleWatchLoop,
а write limit остаётся минимумом caller и idle. Обычный forwarding неизменён.
Retained DATA Read использует один снимок remote EOF: приход FIN между проверками
не должен переводить сохранённый receiver в обычное закрытие.

Новый caller deadline должен прерывать уже ожидающий KCP Read/Write; после
clear/rearm timer channel восстанавливается. Не менять raw Timeout/EOF/EPIPE,
исходные close budgets, FIN/ACK/Abort и retained read retirement. Физическая
приёмка текущего результата r4513 остаётся FAIL: 28 Close, 6 EPIPE, media verdict.
