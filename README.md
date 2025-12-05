# Tablet Reminder Bot

Телеграм-бот, который помогает не забыть про таблетки. Написан на Python 3 + aiogram 2.x, хранит данные в PostgreSQL.

## Быстрый старт

1. Создай виртуальное окружение и поставь зависимости:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt  # aiogram, psycopg2, alembic, python-dotenv
   ```
2. Скопируй `.env.example` в `.env` и заполни значения.
3. Подними PostgreSQL и прогоняй SQL‑миграции по порядку (см. раздел ниже).
4. Запусти бота:
   ```bash
   python main.py
   ```

## Конфигурация

| Переменная               | Назначение                                 | Значение по умолчанию |
|-------------------------|---------------------------------------------|-----------------------|
| `BOT_TOKEN`             | токен Telegram‑бота                         | —                     |
| `DB_HOST/PORT/NAME/...` | параметры подключения к PostgreSQL          | `127.0.0.1:5432`      |
| `DB_POOL_MIN/MAX`       | размер пула соединений                      | `1 / 5`               |
| `TZ`                    | дефолтный часовой пояс новых пользователей  | `Europe/Moscow`       |
| `REMIND_BEFORE_MINUTES` | за сколько минут до приёма слать напоминание| `10`                  |
| `SNOOZE_MINUTES`        | шаг повторных напоминаний «Напомни позже»   | `15`                  |
| `ADMIN_CHAT_ID`         | чат для алертов об ошибках                  | пусто (отключено)     |
| `SUMMARY_HOUR`          | локальное время отправки ежедневного отчёта | `21`                  |

## Миграции

Схема поддерживается SQL‑миграциями в `migrations/`. Действуй так:

1. Перед обновлением сделай резервную копию (`pg_dump`).
2. Выполняй файлы по возрастанию номера:
   ```bash
   psql "$DATABASE_URL" -f migrations/001_initial.sql
   # далее 002, 003 и т.д.
   ```
3. После применения нового файла обнови `schema.sql`, чтобы он отражал актуальную структуру.

> В директории есть `000_template.sql` — используй как шаблон для новых миграций. При желании можно подвязать Alembic (он уже добавлен в `requirements.txt`), но сейчас достаточно SQL‑файлов.

Текущий список:

- `001_initial.sql` — первичное создание таблиц.
- `002_add_language.sql` — добавляет колонку `language` в `users`.
- `003_add_remind_before.sql` — добавляет настройку `remind_before_minutes`.
- `004_add_paused_until.sql` — даёт `medications` дату автоматического возобновления курса (`paused_until`).
- `005_add_pill_stock.sql` — добавляет дозировку, остаток и флаг уведомления (`pills_per_dose`, `stock_total`, `low_stock_notified`).

## Основные возможности

- Мастер добавления лекарств с указанием количества приёмов, конкретных часов или пресетов по времени суток.
- Напоминания за `REMIND_BEFORE_MINUTES` минут до приёма + кнопки «Выпил», «Напомнить позже», «Не напоминать».
- Snooze каждые `SNOOZE_MINUTES` минут до отметки.
- Управление лекарствами командой `/meds`: редактирование расписания, пауза/возобновление, удаление.
- Учет часового пояса для каждого пользователя, ручная отметка приёмов через кнопку в клавиатуре.
- Ежедневные сводки по прогрессу, учитывающие просроченные приёмы.
- Команды `/help`, `/stats`, `/language` для дружелюбного UX.

## Мониторинг

- При наличии `ADMIN_CHAT_ID` бот отправляет критические ошибки прямо в заданный чат.
- `/stats` показывает базовые метрики (количество напоминаний, snooze, ручных отметок) с момента запуска. При необходимости их можно парсить и прокидывать в сторонний мониторинг.

## Деплой

На VPS с systemd:

1. Создай пользователя, положи код в `/opt/tabletbot`.
2. Настрой `.env` и единый `.service`:
   ```
   [Unit]
   Description=Tablet Reminder Bot
   After=network.target

   [Service]
   Type=simple
   WorkingDirectory=/opt/tabletbot/takingPills
   EnvironmentFile=/opt/tabletbot/takingPills/.env
   ExecStart=/opt/tabletbot/.venv/bin/python main.py
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   ```
3. `sudo systemctl enable --now tabletbot.service`

Логи доступны через `journalctl -u tabletbot -f`.
