# План

1. Новый helper в common/wlt за with_wlt выбирает optional capability.
2. Один вызов в существующем finishClose(false); Abort не изменять.
3. Проверить capability, legacy fallback, исходные ошибки, совместный Close/Abort
   и однократное освобождение, затем прежние WLT пакеты и ordinary gates.
4. После carrier gates и backup опубликовать и зафиксировать точный pin;
   собрать native, пройти новый допуск и физическую проверку Pixel.

Upstream-файлы не меняются. Интеграционную ветку не использовать для разработки.

Scoped successor: новые with_wlt helpers, минимальные поля/швы в WLT-only
option/service/carrier файлах; отдельный dependency pin. Не разрабатывать
на resolved integration. Проверить actual service forwarding, pre-exposure
activation, cancellation/deadline, disposal, один slot/Close owner и Abort.

Продолжение: раздельные caller read/write и idle deadlines под локальным mutex,
только в уже выбранном scoped режиме; исходные setter ошибки сохраняются.
Первая исходная ошибка Read и её bounded provenance неизменяемы после записи.
Двойное наблюдение EOF заменить одним, сохраняя ordinary EOF и original close caps.
Контрольный host interleaving и repeated actual VLESS/KCP precede full gates.

Scoped idle-read successor: сохранить только write-only idle deadline;
Read/shared setters пересылают caller cap без idle подмены. Сохранить применённый
caller cap даже при сбое последующей write-only rebalance. Реальный watchdog
остаётся без изменений. Один fixed KCP для old/candidate causal fixture,
затем immutable pins и полные original regression gates.
