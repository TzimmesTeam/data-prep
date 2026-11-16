# data-prep

Go-микросервис подготовки банковских сделок для последующего расчёта денежных потоков. Он получает стандартное S3 event notification через SQS, читает CSV из `RAW`, преобразует и проверяет сделки, записывает Parquet в `CURATED` и `QUARANTINE`, после чего сообщает `snapshot_id` в control plane.

Сервис не рассчитывает денежные потоки, PV или dNPV. Он не делит поставку на батчи или партиции и не ограничивает число строк: один входной CSV даёт один основной Parquet-файл.

## Что реализовано

- Получение событий `s3:ObjectCreated:*` из SQS с long polling.
- Чтение исходного CSV из S3 с закреплением за `VersionId` или ETag объекта.
- Построчная обработка CSV с разделителем `;`.
- Проверка восьми обязательных колонок и сценарных правил сделок.
- Применение defaults и очистка значений, неприменимых к выбранному сценарию.
- Преобразование 48 известных входных полей в Parquet-схему из 51 колонки.
- Предупреждение и игнорирование неизвестных CSV-колонок без остановки обработки.
- Потоковая запись через временные файлы без загрузки всей поставки в память.
- Один `CURATED` Parquet для принятых сделок и, только при наличии брака, один `QUARANTINE` Parquet с причинами отклонения.
- HTTP callback в control plane после успешной записи результатов.
- Повторная доставка SQS при ошибках обработки и DLQ после настроенного числа попыток.
- Health/readiness endpoints и graceful shutdown.
- Локальные MinIO, LocalStack SQS и control-plane stub в Docker Compose.

## Как работает сервис

```text
CSV загружен в RAW
    → S3 event notification отправлен в SQS
      (локально: MinIO webhook → event-bridge → SQS)
    → data-prep получает сообщение через ReceiveMessage
    → HeadObject/GetObject исходного CSV
    → построчный разбор, преобразование, defaults и валидация
    → CURATED/deals.parquet
    → QUARANTINE/deals.parquet, только если есть отклонённые строки
    → POST {"snapshot_id":"..."} в control plane
    → DeleteMessage
```

Если одно SQS-сообщение содержит несколько S3 records, каждый CSV обрабатывается отдельно. Сообщение удаляется только после успешной обработки всех записей и всех callback.

### Путь входного объекта

Ожидается ключ:

```text
source=<source>/ingest_date=<YYYY-MM-DD>/snapshot_id=<snapshot_id>/<file>.csv
```

Например:

```text
source=core_banking/ingest_date=2026-11-16/snapshot_id=ab12/deals.csv
```

Из него сервис получает параметры результата и `snapshot_id` для callback.

### Пути результата

Для примера выше создаётся:

```text
s3://curated/source=core_banking/ingest_date=2026-11-16/snapshot_id=ab12/deals.parquet
```

Если часть строк отклонена, дополнительно создаётся:

```text
s3://quarantine/source=core_banking/ingest_date=2026-11-16/snapshot_id=ab12/deals.parquet
```

Повторная обработка той же поставки использует те же ключи и перезаписывает объекты. `QUARANTINE` содержит колонки `row`, `deal_id` и `reasons`: номер строки CSV, доступный исходный ID и список причин.

Частичный брак не отменяет обработку. Если все сделки отклонены, сервис всё равно создаёт корректный CURATED Parquet с нулём строк, записывает весь брак в QUARANTINE и отправляет callback.

### Callback

```http
POST <control_plane.url>
Content-Type: application/json

{"snapshot_id":"ab12"}
```

Успешен любой ответ 2xx. Сетевые ошибки и 5xx повторяются внутри client согласно конфигурации. Если callback не выполнен, SQS-сообщение не удаляется.

Доставка имеет семантику at-least-once: callback может повториться, например при остановке между успешным POST и `DeleteMessage`. Control plane должен дедуплицировать уведомления по `snapshot_id`.

## CSV и преобразования

### Заголовок

Обязательны только восемь колонок:

- `report_dttm`;
- `deal_id`;
- `discriminator`;
- `current_principal_amt`;
- `maturity_dttm`;
- `rate_type_cd`;
- `interest_rate`;
- `day_convention_cd`.

Остальные известные колонки можно опускать: они считаются пустыми. Порядок колонок произвольный. Дубликат известной колонки или отсутствие обязательной колонки — ошибка всего CSV. Неизвестная колонка вызывает `WARN`, игнорируется и не мешает обработке.

У каждой принятой сделки восемь минимальных полей непустые. Пустой `currency_id` преобразуется в `NULL`; непустой код должен быть известным ISO 4217 кодом. Нулевой и отрицательный `current_principal_amt` допустимы.

### Скалярные значения

| Поля | Преобразование |
|---|---|
| Все `*_dttm` | Дата `YYYY-MM-DD`, `DD.MM.YYYY` или `DD/MM/YYYY` → `int64` Unix seconds в полночь UTC |
| `deal_id` | `int64` |
| `currency_id` | Буквенный ISO 4217 код → числовой `int32`, например `RUB` → `643` |
| `discriminator` | `ANN` → `1`, `RGM` → `2`, `SIMPLE` → `3` |
| `rate_type_cd`, `sub_rate_type_cd` | `FIXED` → `1`, `FLOAT` → `2` |
| `day_convention_cd` | `ACT/ACT` → `1`, `ACT/ACT ISDA` → `2`, `ACT/365` → `3`, `ACT/360` → `4`, `30/360` → `5`, `30E/360` → `6` |
| `*_day_num`, `sub_repricing_rule_first_event_flg` | `int32` |
| Суммы, ставки, `accrued_interest`, `sub_coeff_index` | Конечное `double`; NaN и бесконечности отклоняются |
| Индикаторы, `product`, `behavior_model_cd` | Строка с удалением пробелов по краям |

Пустое скалярное значение записывается как Parquet `NULL`, а не как ноль или пустая строка.

### Периоды

- `NM` → `N` месяцев;
- `NY` → `N * 12` месяцев;
- `NQ` → `N * 3` месяца;
- `ND` → `1000000 + N` дней;
- `NW` → `1000000 + N * 7` дней;
- `ENDDATE` → `0` для обычных period-полей;
- lag-поля также принимают отрицательный знак, например `-3M` → `-3`, `-5D` → `-1000005`.

При непустом `period_cd != 0` соответствующий start обязателен. При пустом period или `ENDDATE` start и day_num игнорируются и очищаются в `NULL`.

### Списки и параллельные массивы

| CSV-поле | Parquet-поля |
|---|---|
| `interest_payment` | `interest_payment` |
| `principal_payment` | `principal_payment_dates`, `principal_payment_amounts` |
| `repricing_dttm_schedule` | `repricing_dttm_schedule` |
| `repricing_dttm_rate_schedule` | `repricing_dttm_rate_schedule_dates`, `repricing_dttm_rate_schedule_rates` |
| `repricing_dttm_rate_schedule_fixed` | `repricing_dttm_rate_schedule_fixed_dates`, `repricing_dttm_rate_schedule_fixed_rates` |

Элементы разделяются пробелами. Пары записываются как `дата,значение`, например `2022-12-31,250000.0`. Даты сортируются по возрастанию, дубликаты удаляются. Для пар сохраняется связь по индексу; конфликтующие значения на одну дату отклоняют сделку. Пустой список записывается как `[]`, не `NULL`.

### Сценарии, defaults и очистка

- `deal_dttm` обязателен при пустом `accrued_interest`; если accrued interest задан, пустой `deal_dttm` заполняется из `report_dttm`.
- Пустой `deal_report_dttm` заполняется из `report_dttm`.
- ANN требует regular payment start/period; его `interest_payment_rule_*` очищаются.
- RGM обычно использует regular payment rule. При его отсутствии допускается interest calendar; если нет обоих правил, сделка принимается как «всё в конце срока» с `WARN`.
- SIMPLE поддерживает долг в конце срока, regular payments или явный principal schedule; проценты могут задаваться правилом, списком дат или выплачиваться в конце срока.
- FLOAT требует `base_indicator`. У FIXED indicator, cap/floor, lag и repricing-поля не вызывают отклонение, а очищаются.
- Субсидия существует, если задан `sub_rate_type_cd`. Тогда обязательны `sub_interest_rate`, sub payment start/period, а для плавающей субсидии также `sub_float_indicator`.
- Пустой `sub_coeff_index` у плавающей субсидии заполняется значением `1`.
- Если субсидии нет, все `sub_*` очищаются. У FIXED-субсидии очищаются поля плавающей ставки.
- Пустые day_num у interest/regular/sub payment и sub repricing заполняются днём месяца соответствующего start, если правило действует.

Ошибка отдельной строки отправляет только её в QUARANTINE. Также отклоняются некорректные типы и коды, `maturity_dttm <= deal_dttm` и повторный непустой `deal_id` внутри файла.

## Запуск локально

### Требования

- Docker с Docker Compose v2;
- браузер для MinIO Console и Docker Desktop для просмотра логов;
- Go 1.24.9+ только для локальных Go-проверок без Docker.

Из каталога `data-prep` выполните:

```bash
docker compose up --build -d
```

При запуске автоматически создаются bucket’ы `raw`, `curated`, `quarantine`, SQS queue `data-prep-raw-events`, DLQ и notification для CSV в RAW. Текущий MinIO не имеет встроенного SQS target, поэтому локальный `event-bridge` принимает его webhook и без изменения тела пересылает стандартное S3 event notification в SQS. В production это звено заменяется прямой интеграцией S3 с SQS.

Локальные интерфейсы:

- health: `http://localhost:8080/health`;
- readiness: `http://localhost:8080/health/ready`;
- MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`);
- LocalStack SQS endpoint: `http://localhost:4566`;
- control-plane stub: порт `8081`.

Credentials предназначены только для локального запуска.

## Интерактивная проверка на 55 тестовых сделках

Используется `deals.csv` из корня общего проекта. Все 55 сделок должны быть приняты.

### 1. Загрузить CSV через MinIO Console

1. Откройте `http://localhost:9001` и войдите с `minioadmin` / `minioadmin`.
2. В **Object Browser** откройте bucket `raw`.
3. Через **Create Path** создайте путь `source=core_banking/ingest_date=2026-11-16/snapshot_id=ab12` и откройте его.
4. Нажмите **Upload File** и выберите `deals.csv`.
5. Убедитесь, что объект называется `source=core_banking/ingest_date=2026-11-16/snapshot_id=ab12/deals.csv`.

После загрузки ничего отправлять вручную не нужно: notification автоматически попадёт в SQS, а data-prep начнёт обработку.

### 2. Проверить callback

В Docker Desktop откройте проект **data-prep**, затем контейнер **control-plane-stub** и вкладку **Logs**. Найдите POST на `/snapshots/prepared` с телом:

```json
{"snapshot_id":"ab12"}
```

В логах контейнера **data-prep** должно быть сообщение `delivery completed` со значениями `deals_in=55`, `deals_ok=55`, `deals_rejected=0`. Для сделки 43 допускается ожидаемый `WARN` о сценарии «всё в конце срока».

### 3. Проверить Parquet через MinIO Console

1. Откройте bucket `curated` в **Object Browser**.
2. Перейдите по пути `source=core_banking` → `ingest_date=2026-11-16` → `snapshot_id=ab12`.
3. Убедитесь, что существует ровно один объект `deals.parquet` и скачайте его через меню объекта.
4. Откройте скачанный файл в Parquet viewer или редакторе с поддержкой Parquet. Должно быть 55 строк и 51 колонка; `deal_id` содержит значения от 1 до 55.
5. Проверьте, что вместо трёх составных CSV-полей присутствуют шесть параллельных массивов, пустые скаляры имеют `NULL`, а пустые списки — `[]`.
6. В bucket `quarantine` объект `snapshot_id=ab12/deals.parquet` для исходного файла должен отсутствовать.

## Обработка ошибок

- Некорректное событие, ошибка схемы CSV, S3, Parquet или callback: сообщение не удаляется и становится доступным после visibility timeout.
- После пяти получений неуспешное сообщение переносится LocalStack в `data-prep-raw-events-dlq` согласно redrive policy.
- Отклонённые сделки не являются ошибкой всей поставки: результаты записываются, callback отправляется, сообщение удаляется.
- Если SQS недоступен, readiness возвращает `503`; consumer автоматически возобновляет polling после восстановления соединения.

## Полезные команды

```bash
docker compose logs -f data-prep
docker compose config --quiet
go vet ./...
go test ./...
go build ./...
```

Остановка с сохранением данных:

```bash
docker compose down
```

Остановка с удалением локальных данных MinIO и LocalStack:

```bash
docker compose down -v
```

## Конфигурация

Локальные параметры находятся в `config/local.yml`: S3 endpoints и три bucket’а, SQS queue/polling/visibility, callback URL/timeouts/retries и HTTP server. Для других окружений инфраструктура создаёт bucket’ы, queue, DLQ и bucket notification заранее.

Настройки и credentials можно переопределить переменными окружения:

- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`;
- `S3_RAW_BUCKET`, `S3_CURATED_BUCKET`, `S3_QUARANTINE_BUCKET`;
- `SQS_QUEUE_URL`, `SQS_ACCESS_KEY_ID`, `SQS_SECRET_ACCESS_KEY`;
- `CONTROL_PLANE_URL`;
- `CONFIG_PATH` — путь к YAML-конфигурации.

Путь к конфигурации также можно задать флагом `-config`. Реальные секреты не должны попадать в `config/local.yml` или коммититься.
