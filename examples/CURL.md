# Проверка через curl в PowerShell

Запускайте команды из корня проекта после запуска сервисов. Используется `curl.exe`: в Windows PowerShell 5.1 имя `curl` может быть псевдонимом другой команды. JSON передаётся из файлов, чтобы избежать проблем экранирования кавычек.

Автоматическая проверка двух сценариев:

```powershell
.\examples\smoke.ps1
# При другом адресе или ключе:
.\examples\smoke.ps1 -BaseUrl 'http://localhost:8000' -ApiKey 'your-api-key'
```

Скрипт создаёт новый ключ идемпотентности при каждом запуске. Первый сценарий проверяет создание (`202`) и окончательный статус через `GET`, включая заполненный `processed_at`. Второй — эквивалентный повтор (`200`), конфликт изменённых данных (`409`) и неизменность платежа через финальный `GET`. Ожидание окончательного статуса ограничено 30 секундами. Коды завершения: `0` — оба сценария прошли; `1` — ошибка; `2` — обработка не завершилась за отведённое время. Окончательный `failed` допустим: эмулятор может отказать в оплате. Скрипт выводит ключ идемпотентности и JSON первого и последнего ответов для отчёта; API-ключ не выводится. Успех этого скрипта не доказывает доставку webhook; её проверяют отдельно по логам получателя.

Те же запросы вручную:

```powershell
$BaseUrl = 'http://localhost:8000'
$ApiKey = 'dev-api-key'
$Key = [Guid]::NewGuid().ToString()

# 1. Новый платёж: ожидается 202, payment_id, status=pending.
curl.exe -i --max-time 10 -X POST "$BaseUrl/api/v1/payments" -H "X-API-Key: $ApiKey" -H "Idempotency-Key: $Key" -H 'Content-Type: application/json' --data-binary '@examples/payment-create.json'

# Скопируйте payment_id из ответа выше.
$PaymentId = 'paste_payment_id_here'

# Проверяйте GET до succeeded или failed; обычно обработка занимает несколько секунд.
curl.exe -i --max-time 10 "$BaseUrl/api/v1/payments/$PaymentId" -H "X-API-Key: $ApiKey"

# 2. Тот же ключ, эквивалентные данные: ожидается 200, прежний ID и текущий статус.
curl.exe -i --max-time 10 -X POST "$BaseUrl/api/v1/payments" -H "X-API-Key: $ApiKey" -H "Idempotency-Key: $Key" -H 'Content-Type: application/json' --data-binary '@examples/payment-equivalent.json'

# 3. Тот же ключ, изменено описание: ожидается 409.
curl.exe -i --max-time 10 -X POST "$BaseUrl/api/v1/payments" -H "X-API-Key: $ApiKey" -H "Idempotency-Key: $Key" -H 'Content-Type: application/json' --data-binary '@examples/payment-conflict.json'

# 4. Существующий платёж должен остаться неизменным.
curl.exe -i --max-time 10 "$BaseUrl/api/v1/payments/$PaymentId" -H "X-API-Key: $ApiKey"
```

`payment-equivalent.json` меняет порядок полей и представление суммы (`"100.00"` → `100.0`), сохраняя тот же нормализованный запрос. `payment-conflict.json` меняет только описание. Во всех файлах webhook направлен на контейнер `http://webhook-sink:8002/webhooks`.
