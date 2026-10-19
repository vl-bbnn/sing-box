# Отчёт

Локальный API проверен в r4090: normal/race, прежние WLT target tests,
default tests и vet. Carrier прошёл все 1649 прежних и 30 новых проверок в
трёх режимах r4088, штатный check/vet, восстановление r4089/r4092 и публикацию
отдельной feature-ветки r4093. Здесь фиксируется точный опубликованный pin.

Повторные проверки выполняются по настоящему go.mod без локального carrier
replace. Старые тесты и остальные зависимости сохранены. Существующий
wireguard submodule восстановлен в исходном относительном расположении.

r4094 прошёл normal, но FAILED в race: неизменный reserve maintenance test
не дождался следующей проверки за исходные 3 секунды. Отрицательный результат
сохранён. r4103: по 10 исходных повторений на старом и новом carrier в normal/race
прошли. r4104 наблюдает File.Sync на обоих исходниках; исходная причина задержки
r4094 не установлена. Здесь требуется новый полный PASS с теми же тестами и
сроками без observer overlay. Go runtime source и tests не меняются.

Задача остаётся OPEN: native сборка и новая физическая приёмка ещё не выполнены.
Ни телефоны, ни серверы, ни обычные/default ветки этим шагом не меняются.


## Scoped application read retirement

Добавлен отдельный `scoped_application_read_retirement`, default false, только
with_wlt; он требует data_half_close. Активация capability происходит до
публикации потока, с исходным dial context; отказ завершает исходное соединение
и освобождает admission. Выбор Close хранится на конкретном потоке. Legacy
helper и immediate Abort сохранены; исходные ошибки не нормализуются.

Host: все 368 прежних WLT случаев плюс 17 новых проходят normal и race;
32 default случая неизменны, оба vet проходят. Проверены JSON/service bridge,
обычный Read после активации, один Close/slot owner, raw admission/disposal
ошибки, cancellation/deadline и generation/Close/Abort races. Обычные target
зависимости не включают carrier/KCP. Native/physical и resolved integration
приёмка остаются открытыми; host pass не является полевой квалификацией.

## Scoped idle read ownership и pending caller deadlines

В scoped режиме Read deadline принадлежит caller. Idle activity меняет только
write limit, а существующий watchdog владеет idle Close и передачей чтения.
Carrier/Close/retirement и прежние write budgets не менялись. Явные и неизвестные
начальные read caps, исходные Timeout/EOF/EPIPE не скрываются.

KCP prerequisite: исправлены wake-up из initial-zero и channel rearm после clear;
232 normal/race, исходные 1/6 GiB, два negative mutants и vet прошли. Carrier
с неизменённым runtime прошёл 1711 identities в трёх режимах и mandatory check.
Whole-mux fixture теперь требует фактического завершения original Close и DATA
workers, исходных nil errors и неизменённого cap, сохраняя raw aborted/session_closed.
Старый и новый KCP воспроизвели прежний racy status assertion; 160 наблюдений
подтвердили фактические join, а исходный failed run сохранён.

Core: 470 original/new identities normal/race, 32 default и оба vet пройдены
с точными опубликованными зависимостями. Раздельные атомарные коммиты содержат
WLT policy/tests/docs и помеченные dependency pins. Другие upstream зоны не
менялись; риска ребейза добавляют только уже существующие WLT dependency строки.

Задача остаётся open: нужны source recovery, merge-only product integration,
ordinary/tagged/native builds и новая физическая приёмка. Предыдущие 28 client
Close errors и 6 server EPIPE остаются FAIL; это разные наборы flows. Строгий
browser verdict также остаётся FAIL. Ускорение и полная стабильность не заявлены.
