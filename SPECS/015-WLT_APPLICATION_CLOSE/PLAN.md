# План

1. Новый helper в common/wlt за with_wlt выбирает optional capability.
2. Один вызов в существующем finishClose(false); Abort не изменять.
3. Проверить capability, legacy fallback, исходные ошибки, совместный Close/Abort
   и однократное освобождение, затем прежние WLT пакеты и ordinary gates.
4. После carrier gates и backup опубликовать и зафиксировать точный pin;
   собрать native, пройти новый допуск и физическую проверку Pixel.

Upstream-файлы не меняются. Интеграционную ветку не использовать для разработки.
