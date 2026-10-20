# План

Новые with_wlt option/group/common-urltest файлы; обычные stubs не признают
поле wlt_body_probe. Минимальные lx-швы в URLTest options/group lifecycle,
selection и fallback. История body хранится по configured member с leaf,
interface generation и временем; memory bounded количеством members.
Старая общая HEAD history остаётся нетронутой. Отдельный GET transport без
reuse/redirect/replay с исходнымTLS trust, exact bounded response и сохранённым
результатом underlyingClose. Шаги ниже фиксируются в TASKS; никакой установки
до полного local/source/recovery gate.
