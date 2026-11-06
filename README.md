# data-prep

Go-микросервис подготовки банковских сделок для последующего расчёта денежных потоков. Он получает задание через NATS JetStream, читает CSV из S3/MinIO, преобразует значения в типизированный формат, сохраняет NDJSON и manifest в S3 и публикует итоговое уведомление.

Сервис не рассчитывает денежные потоки, PV или dNPV и не обращается напрямую к control plane. Подготовленные данные использует отдельный calculation worker.

## Что реализовано

- Потоковая обработка CSV без загрузки всего батча в память.
- Строгая проверка наличия всех 48 известных колонок.
- Предупреждение и игнорирование неизвестных колонок без остановки обработки.
- Преобразование дат в Unix seconds UTC, кодов в числа, сумм и ставок в `float64`.
- Преобразование списков дат и пар «дата,значение» в JSON-массивы.
- Сохранение пустых значений как JSON `null`; ключи записи не опускаются.
- Потоковая запись NDJSON-партиций принятых и отклонённых сделок в S3.
- Формирование manifest с описанием источника, партиций и статистики.
- Привязка результата к версии исходного объекта S3.
- JetStream WorkQueue для заданий, явные acknowledgements и повторные доставки.
- Публикация результата с подтверждением JetStream до acknowledgement задания.
- Отдельный subject для заданий, исчерпавших лимит доставок.
- Автоматическое переподключение к NATS, health/readiness endpoints и graceful shutdown.
- Локальные MinIO и NATS с JetStream в Docker Compose.

## Как работает сервис

```text
JetStream: data-prep.tasks
    → HeadObject/GetObject CSV в S3
    → построчный разбор, преобразование и валидация
    → NDJSON-партиции deals/rejected в S3
    → manifest.json в S3
    → JetStream: data-prep.results.completed
    → ack исходного задания после PubAck результата
```

Через JetStream передаются только небольшие задания и итоговые уведомления, а не сами сделки. Подготовленные данные остаются в S3.

Manifest загружается последним и является признаком полностью подготовленного результата. Объекты без manifest не считаются завершённым батчем.

### Входное задание

Сообщение публикуется в subject `data-prep.tasks`:

```json
{
  "run_id": "11111111-1111-4111-8111-111111111111",
  "batch_id": "22222222-2222-4222-8222-222222222222",
  "attempt": 1,
  "source_uri": "s3://data-lake/input/deals.csv",
  "output_prefix": "s3://data-lake/prepared/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/attempt=1"
}
```

Требования:

- `run_id` и `batch_id` — UUID;
- `attempt` — положительное число;
- `source_uri` и `output_prefix` — URI вида `s3://bucket/key`;
- `output_prefix` заканчивается на `attempt=N`, где `N` совпадает с полем `attempt`;
- `source_uri` указывает на полный CSV со сделками.

### Версия источника и пути результата

Перед чтением сервис выполняет `HeadObject` и определяет `source_revision`:

- S3 `VersionId`, если включено версионирование;
- иначе ETag объекта.

Чтение закрепляется за этой версией через `VersionId` или `IfMatch`. В ключе результата revision представлен в URL-safe Base64:

```text
{output_prefix}/source={source_revision}/deals/part-000001.ndjson
{output_prefix}/source={source_revision}/rejected/part-000001.ndjson
{output_prefix}/source={source_revision}/manifest.json
```

Повтор задания над неизменным CSV использует те же ключи. Если CSV перезаписали, появляется новый revision, и прежний результат не перезаписывается.

### Manifest

```json
{
  "schema_version": 1,
  "run_id": "11111111-1111-4111-8111-111111111111",
  "batch_id": "22222222-2222-4222-8222-222222222222",
  "attempt": 1,
  "source": {
    "uri": "s3://data-lake/input/deals.csv",
    "revision": "etag-or-version-id",
    "etag": "etag",
    "size_bytes": 12733
  },
  "deals": [
    {
      "uri": "s3://data-lake/prepared/.../deals/part-000001.ndjson",
      "size_bytes": 36425,
      "rows": 55
    }
  ],
  "rejected": [],
  "stats": {
    "deals_in": 55,
    "deals_ok": 55,
    "deals_rejected": 0,
    "parts_out": 1
  }
}
```

`parts_out` учитывает только deals-партиции.

### Итоговое уведомление

После записи manifest сервис публикует JSON в `data-prep.results.completed`:

```json
{
  "run_id": "11111111-1111-4111-8111-111111111111",
  "batch_id": "22222222-2222-4222-8222-222222222222",
  "attempt": 1,
  "source_revision": "etag-or-version-id",
  "status": "success",
  "manifest_uri": "s3://data-lake/prepared/.../manifest.json",
  "stats": {
    "deals_in": 55,
    "deals_ok": 55,
    "deals_rejected": 0,
    "parts_out": 1
  }
}
```

`Nats-Msg-Id` формируется из `run_id/batch_id/attempt/source_revision`. Доставка работает по модели at-least-once, поэтому потребитель должен дедуплицировать уведомления.

## Формат CSV и преобразование полей

Разделитель CSV — `;`. Все 48 известных колонок обязаны присутствовать в заголовке, но их порядок произвольный. Пустое значение в ячейке не является само по себе ошибкой: соответствующее выходное поле получает значение `null`.

Отсутствие любой известной колонки или её дубликат — ошибка схемы всего CSV с итоговым `status:error`. Неизвестная колонка вызывает `WARN`, игнорируется и не попадает в NDJSON; дубликат неизвестной колонки также приводит только к предупреждению.

### Скалярные поля

| Исходные поля | Правило преобразования |
|---|---|
| `report_dttm`, `deal_dttm`, `maturity_dttm`, все `*_rule_start_dttm`, `deal_report_dttm` | Дата → Unix seconds в полночь UTC |
| `discriminator` | `ANN` → `1`, `RGM` → `2`, `SIMPLE` → `3` |
| `deal_id` | `int64` |
| `currency_id` | Буквенный код ISO 4217 → числовой код, например `RUB` → `643`, `USD` → `840` |
| `rate_type_cd`, `sub_rate_type_cd` | `FIXED` → `1`, `FLOAT` → `2` |
| `day_convention_cd` | `ACT/ACT` → `1`, `ACT/ACT ISDA` → `2`, `ACT/365` → `3`, `ACT/360` → `4`, `30/360` → `5`, `30E/360` → `6` |
| `interest_payment_rule_period_cd`, `regular_payment_rule_period_cd`, `repricing_rule_period_cd`, `sub_payment_rule_period_cd`, `sub_repricing_rule_period_cd` | Период → числовой код, см. ниже |
| `indicator_lag_cd`, `sub_indicator_lag_cd` | Положительный или отрицательный период → числовой код |
| `interest_payment_rule_day_num`, `regular_payment_rule_day_num`, `repricing_rule_day_num`, `sub_payment_rule_day_num`, `sub_repricing_rule_day_num`, `sub_repricing_rule_first_event_flg` | `int32` |
| Все `*_amt`, `*_rate`, `accrued_interest`, `sub_coeff_index` | Конечное `float64`, включая ноль и отрицательные значения |
| `base_indicator`, `product`, `behavior_model_cd`, `sub_float_indicator` | Строка без изменения содержимого, кроме удаления пробелов по краям |

Даты принимаются в форматах `YYYY-MM-DD`, `DD.MM.YYYY` и `DD/MM/YYYY`. Например, `2022-08-31` преобразуется в `1661904000`.

Коды периодов:

- `NM` → `N` месяцев, например `3M` → `3`;
- `NY` → `N * 12` месяцев, например `2Y` → `24`;
- `ND` → `1000000 + N` дней, например `10D` → `1000010`;
- `ENDDATE` → `0`, только для обычных period-полей;
- lag-поля дополнительно принимают знак минус, например `-3M` → `-3`.

Нулевой `current_principal_amt` допустим и сохраняется как число `0`.

### Списки и параллельные массивы

| Исходное поле | Выходные поля | Правило |
|---|---|---|
| `interest_payment` | `interest_payment` | Даты через пробел; сортировка по возрастанию и удаление дубликатов |
| `principal_payment` | `principal_payment_dates`, `principal_payment_amounts` | Элементы `YYYY-MM-DD,amount`; исходный порядок пар сохраняется |
| `repricing_dttm_schedule` | `repricing_dttm_schedule` | Даты через пробел; сортировка по возрастанию и удаление дубликатов |
| `repricing_dttm_rate_schedule` | `repricing_dttm_rate_schedule_dates`, `repricing_dttm_rate_schedule_rates` | Элементы `date,rate`; стабильная сортировка пар по дате |
| `repricing_dttm_rate_schedule_fixed` | `repricing_dttm_rate_schedule_fixed_dates`, `repricing_dttm_rate_schedule_fixed_rates` | Элементы `date,rate`; стабильная сортировка пар по дате |

В записях NDJSON всегда присутствует 51 ключ: 48 исходных имён минус три исходных поля с парами плюс шесть полей параллельных массивов. Для пустого исходного списка его выходное значение — `null`. Для заполненных пар длины `*_dates` и соответствующих `*_amounts`/`*_rates` совпадают.

Пример фрагмента записи:

```json
{
  "deal_id": 16,
  "report_dttm": 1661904000,
  "start_principal_amt": null,
  "principal_payment_dates": [1672444800],
  "principal_payment_amounts": [250000]
}
```

### Проверки сделок

Data-prep не применяет бизнес-defaults и не требует заполнения отдельных групп полей. Отклоняются только строки с некорректными значениями или нарушением следующих правил:

- значение не соответствует указанному выше типу, коду или формату;
- `maturity_dttm <= deal_dttm`, если обе даты заполнены;
- непустой `deal_id` повторяется внутри одного CSV.

Пустой `deal_id` сериализуется как `null` и не участвует в проверке дубликатов. Отклонённая строка записывается в rejected NDJSON с номером строки CSV, исходным `deal_id`, если он доступен, и списком причин. Ошибки отдельных строк не отменяют остальные сделки. Если все строки отклонены, итоговый статус остаётся `success`, а список deals-партиций пустой.

Лимиты по умолчанию:

- 100 000 входных строк;
- 1 000 записей в партиции;
- 8 MiB на партицию;
- первые 10 отклонённых строк логируются отдельно, остальные учитываются агрегированно.

## JetStream и обработка ошибок

При запуске сервис создаёт или обновляет:

| Ресурс | Назначение |
|---|---|
| Stream `DATA_PREP_TASKS` | Subject `data-prep.tasks`, `WorkQueuePolicy`, файловое хранилище |
| Durable pull consumer `data-prep-workers` | Явный acknowledgement, `MaxDeliver=5` |
| Stream `DATA_PREP_RESULTS` | Subjects `data-prep.results.completed` и `data-prep.tasks.failed`, файловое хранилище |

Обработка ошибок:

- Некорректный JSON, контракт задания или схема CSV: публикация `status:error` и acknowledgement, без повторной доставки.
- Временная ошибка S3 или обработки: `NakWithDelay`; задание доставляется повторно до пяти раз.
- После пятой неуспешной доставки публикуется `status:error`, исходное задание копируется в `data-prep.tasks.failed`, затем подтверждается в task stream.
- При успехе acknowledgement выполняется только после записи manifest и получения JetStream PubAck итогового уведомления.
- `attempt` не изменяется сервисом: это бизнес-попытка, которую увеличивает внешний инициатор.

При потере соединения readiness возвращает `503`; сервис автоматически переподключается и восстанавливает потребление.

## Запуск локально

### Требования

- Docker с Docker Compose v2;
- браузер для MinIO Console и проверки health endpoints;
- Go 1.24+ только для локальных Go-проверок без Docker.

Команду запуска выполните из каталога `data-prep`:

```bash
docker compose up --build -d
```

После запуска откройте в браузере `http://localhost:8080/health/ready`. Ожидаемый ответ:

```json
{"status":"ready"}
```

При старте `bucket-init` создаёт bucket `data-lake`. Сам `data-prep` bucket не создаёт: он проверяет его через `HeadBucket` и завершает запуск с ошибкой, если bucket недоступен.

Локальные интерфейсы:

- health: `http://localhost:8080/health`;
- readiness: `http://localhost:8080/health/ready`;
- MinIO API: `http://localhost:9000`;
- MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`);
- NATS: `nats://localhost:4222`;
- NATS monitoring: `http://localhost:8222` и `http://localhost:8222/jsz`.

Эти credentials предназначены только для локального запуска. Веб-интерфейс NATS на порту `8222` предназначен для мониторинга и не позволяет публиковать сообщения. Для интерактивной работы с заданиями ниже используется консоль `nats-box` с NATS CLI.

## Интерактивная проверка на тестовых данных

Используется файл `deals.csv` из корня общего проекта: 55 сделок, 48 колонок. Все сделки должны быть приняты.

### 1. Загрузить CSV через MinIO Console

1. Откройте `http://localhost:9001` и войдите с логином `minioadmin` и паролем `minioadmin`.
2. Перейдите в уже созданный bucket `data-lake`.
3. Создайте путь `input` через **Create Path** и откройте его.
4. Нажмите **Upload File** и выберите `deals.csv` из корня общего проекта.
5. Убедитесь, что в Object Browser появился объект `input/deals.csv`. Его S3 URI для задания: `s3://data-lake/input/deals.csv`.

### 2. Открыть NATS-консоль и опубликовать задание

Один раз откройте консоль `nats-box`:

```bash
docker run --rm -it --network data-prep_default --entrypoint /bin/sh natsio/nats-box:latest
```

Все команды NATS из этой инструкции вводятся уже внутри открывшейся консоли. Для подключения к локальному NATS используется адрес `nats://nats:4222`, а не `localhost`.

Опубликуйте задание одной командой:

```bash
nats pub --server nats://nats:4222 --jetstream data-prep.tasks '{"run_id":"11111111-1111-4111-8111-111111111111","batch_id":"22222222-2222-4222-8222-222222222222","attempt":1,"source_uri":"s3://data-lake/input/deals.csv","output_prefix":"s3://data-lake/prepared/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/attempt=1"}'
```

JetStream должен подтвердить публикацию acknowledgement. Не закрывайте эту консоль: она понадобится для чтения результата и следующих проверок.

### 3. Прочитать итоговое уведомление в NATS-консоли

Через несколько секунд после публикации выполните в той же консоли:

```bash
nats stream get DATA_PREP_RESULTS --last-for=data-prep.results.completed --server nats://nats:4222
```

CLI покажет метаданные и JSON payload в читаемом виде. Проверьте, что `run_id` и `batch_id` относятся к только что отправленному заданию. Для исходного файла payload должен содержать:

```json
{
  "status": "success",
  "stats": {
    "deals_in": 55,
    "deals_ok": 55,
    "deals_rejected": 0,
    "parts_out": 1
  }
}
```

Также в payload будет `manifest_uri` — он понадобится на следующем шаге. Если результат ещё не появился, повторите `stream get`. Логи можно посмотреть без терминала: в Docker Desktop откройте проект **data-prep**, затем контейнер **data-prep-data-prep-1** и вкладку **Logs**.

### 4. Открыть manifest и NDJSON через MinIO Console

1. Возьмите `manifest_uri` из payload в NATS-консоли. Он имеет вид `s3://data-lake/prepared/<run_id>/<batch_id>/attempt=1/source=<revision>/manifest.json`.
2. Вернитесь в MinIO Console и в **Object Browser** откройте bucket `data-lake`. Последовательно перейдите по каталогам из этого URI: `prepared` → нужный `run_id` → `batch_id` → `attempt=1` → `source=<revision>`.
3. Откройте предпросмотр `manifest.json` либо скачайте его через меню объекта и откройте в редакторе.
4. Убедитесь, что `schema_version` равен `1`, статистика содержит `55 / 55 / 0 / 1`, массив `deals` содержит одну партицию с `rows: 55`, а `rejected` пуст.
5. В элементе `deals[0]` найдите `uri` NDJSON-партиции и откройте этот объект в том же Object Browser. Скачайте файл и откройте его в текстовом редакторе.

NDJSON должен содержать 55 строк, по одной сделке с `deal_id` от `1` до `55`. Для визуальной проверки преобразований:

- у первой сделки `"report_dttm":1661904000`, `"start_principal_amt":null`;
- у сделки `16` первый элемент `principal_payment_dates` равен `1672444800`, а первый элемент `principal_payment_amounts` — `250000`;
- пустые исходные ячейки представлены как `null`, а не отсутствующими ключами;
- вместо `principal_payment` присутствуют два массива `principal_payment_dates` и `principal_payment_amounts`; аналогично представлены оба repricing rate schedule.

```bash
nats stream get DATA_PREP_RESULTS --last-for=data-prep.tasks.failed --server nats://nats:4222
```

После завершения проверок введите `exit`, чтобы закрыть интерактивную NATS-консоль.

## Полезные команды

Логи сервиса:

```bash
docker compose logs -f data-prep
```

Состояние JetStream можно посмотреть в той же NATS-консоли:

```bash
nats stream ls --server nats://nats:4222
nats consumer info DATA_PREP_TASKS data-prep-workers --server nats://nats:4222
```

Для веб-мониторинга JetStream откройте `http://localhost:8222/jsz` в браузере.

Проверка Compose-файла и Go-проверки:

```bash
docker compose config --quiet
go fmt ./...
go vet ./...
go test ./...
go build ./...
```

Остановка с сохранением данных:

```bash
docker compose down
```

Остановка с удалением локальных данных MinIO и NATS:

```bash
docker compose down -v
```

## Конфигурация

Локальные параметры находятся в `config/local.yml`: S3 endpoints и buckets, JetStream topology, лимиты батча и партиций, parallelism, delivery limit и timeouts. `ack_wait` должен быть больше `task_timeout`, чтобы задание не было повторно доставлено во время штатной обработки.

Для других окружений bucket создаётся инфраструктурой, а настройки и credentials можно передать через переменные окружения:

- `S3_ACCESS_KEY_ID`;
- `S3_SECRET_ACCESS_KEY`;
- `S3_BUCKETS` — список через запятую;
- `NATS_URL`;
- `CONFIG_PATH` — путь к YAML-конфигурации.

Путь к конфигурации также можно задать флагом `-config`. Реальные секреты не должны попадать в `config/local.yml` или коммититься в `.env`.