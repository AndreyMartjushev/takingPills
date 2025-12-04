import asyncio
import logging
import os
from datetime import date, datetime, timedelta, timezone, time as dtime
from functools import lru_cache
from typing import Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from dotenv import load_dotenv
from db import db_query, init_db_pool

# ----------------- Настройка -----------------
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tabletbot")

BOT_TOKEN = os.getenv("BOT_TOKEN")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", 5))

DEFAULT_TZ_NAME = os.getenv("TZ", "Europe/Moscow")
FALLBACK_TZ_NAME = "UTC"

try:
    DEFAULT_ZONE = ZoneInfo(DEFAULT_TZ_NAME)
except ZoneInfoNotFoundError:
    logger.warning("Unknown TZ %s, falling back to UTC", DEFAULT_TZ_NAME)
    DEFAULT_TZ_NAME = FALLBACK_TZ_NAME
    DEFAULT_ZONE = ZoneInfo(FALLBACK_TZ_NAME)

REMIND_BEFORE_MINUTES = int(os.getenv("REMIND_BEFORE_MINUTES", 10))
SNOOZE_MINUTES = int(os.getenv("SNOOZE_MINUTES", 15))
SUMMARY_HOUR = int(os.getenv("SUMMARY_HOUR", 21))

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
if ADMIN_CHAT_ID:
    try:
        ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
    except ValueError:
        logger.warning("ADMIN_CHAT_ID should be integer, ignoring value %s", ADMIN_CHAT_ID)
        ADMIN_CHAT_ID = None

MANUAL_MARK_BUTTON = "✅ Отметить приём"
ADD_MED_BUTTON = "➕ Добавить лекарство"

SCHEDULE_TYPE_EXACT = "exact"
SCHEDULE_TYPE_PERIOD = "period"

LANGUAGE_OPTIONS = {
    "ru": "Русский",
    "en": "English",
}

DAY_PERIOD_PRESETS = [
    {"key": "morning", "title": "🌅 Утро", "time": dtime(hour=8, minute=0)},
    {"key": "lunch", "title": "🍽 Обед", "time": dtime(hour=13, minute=0)},
    {"key": "day", "title": "🌤 День", "time": dtime(hour=16, minute=0)},
    {"key": "evening", "title": "🌇 Вечер", "time": dtime(hour=20, minute=0)},
    {"key": "night", "title": "🌙 Поздний вечер", "time": dtime(hour=22, minute=30)},
]

HELP_TEXTS = {
    "ru": (
        "Доступные команды:\n"
        "/add — добавить лекарство\n"
        "/list — список и прогресс на сегодня\n"
        "/meds — изменить расписание, паузу или удалить\n"
        "/timezone — сменить часовой пояс\n"
        "/remind — настроить, за сколько минут напоминать\n"
        "/daily — прислать сводку за сегодня\n"
        "/language — выбрать язык\n"
        "/stats — метрики (для администратора)\n\n"
        "Не забудь про кнопку внизу чата: она позволяет отметить приём вручную."
    ),
    "en": (
        "Commands:\n"
        "/add — add a medication\n"
        "/list — show today's progress\n"
        "/meds — manage schedule / pause / delete\n"
        "/timezone — change timezone\n"
        "/remind — configure reminder lead time\n"
        "/daily — send today's summary\n"
        "/language — pick interface language\n"
        "/stats — metrics (admin only)\n\n"
        "Use the bottom button to mark an intake manually when needed."
    ),
}

SUMMARY_STATE: Dict[int, date] = {}
METRICS = {
    "reminders_sent": 0,
    "reminders_failed": 0,
    "intakes_marked": 0,
    "snoozes": 0,
    "missed": 0,
}

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
init_db_pool(DB_CONFIG, minconn=DB_POOL_MIN, maxconn=DB_POOL_MAX)


# ----------------- FSM для добавления лекарства -----------------
class AddMedStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_times_per_day = State()
    waiting_for_schedule_type = State()
    waiting_for_exact_time = State()
    waiting_for_day_period = State()
    waiting_for_remind_before = State()
    confirming_more = State()


# ----------------- Работа со временем -----------------
@lru_cache(maxsize=128)
def _load_zone(tz_name: str) -> ZoneInfo:
    return ZoneInfo(tz_name)


def is_valid_timezone(tz_name: str) -> bool:
    try:
        _load_zone(tz_name)
        return True
    except ZoneInfoNotFoundError:
        return False


def resolve_timezone(tz_name: Optional[str]) -> ZoneInfo:
    if not tz_name:
        return DEFAULT_ZONE
    try:
        return _load_zone(tz_name)
    except ZoneInfoNotFoundError:
        logger.warning("Unknown TZ %s, fallback to %s", tz_name, DEFAULT_TZ_NAME)
        return DEFAULT_ZONE


def get_zone_for_user(user: dict) -> ZoneInfo:
    return resolve_timezone(user.get("timezone"))


def get_remind_before(user: dict) -> int:
    try:
        value = int(user.get("remind_before_minutes", REMIND_BEFORE_MINUTES))
        return max(1, min(180, value))
    except (TypeError, ValueError):
        return REMIND_BEFORE_MINUTES


def get_language_for_user(user: dict) -> str:
    lang = (user.get("language") or "ru").lower()
    if lang in LANGUAGE_OPTIONS:
        return lang
    return "ru"


def get_local_today(zone: ZoneInfo) -> date:
    return datetime.now(zone).date()


def get_day_bounds_local(target_date: date, zone: ZoneInfo):
    day_start = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        tzinfo=zone,
    )
    return day_start, day_start + timedelta(days=1)


def get_day_bounds_utc(target_date: date, zone: ZoneInfo):
    start_local, end_local = get_day_bounds_local(target_date, zone)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def local_time_to_utc(target_date: date, time_str: str, zone: ZoneInfo) -> datetime:
    hour, minute = map(int, time_str.split(":"))
    local_dt = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        hour,
        minute,
        tzinfo=zone,
    )
    return local_dt.astimezone(timezone.utc)


def to_local(dt: datetime, zone: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).astimezone(zone)
    return dt.astimezone(zone)


async def notify_admin(text: str):
    if not ADMIN_CHAT_ID:
        return
    try:
        await bot.send_message(ADMIN_CHAT_ID, f"[ALERT] {text}")
    except Exception as exc:
        logger.warning("Failed to notify admin: %s", exc)


def get_period_by_key(key: str):
    for preset in DAY_PERIOD_PRESETS:
        if preset["key"] == key:
            return preset
    return None


def format_period_label(key: Optional[str], time_str: str) -> str:
    if not key:
        return time_str
    preset = get_period_by_key(key)
    if not preset:
        return time_str
    return f"{preset['title']} ({time_str})"


def format_med_schedule(med) -> str:
    periods = med.get("periods") or []
    times = med.get("times") or []
    if med.get("schedule_mode") == SCHEDULE_TYPE_PERIOD and periods:
        formatted = []
        for idx, t in enumerate(times):
            label = periods[idx] if idx < len(periods) else None
            formatted.append(format_period_label(label, t))
        return ", ".join(formatted) if formatted else "—"
    return ", ".join(times) if times else "—"


def build_schedule_type_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(
            text="🕒 Конкретные часы", callback_data=f"schedule:{SCHEDULE_TYPE_EXACT}"
        ),
        types.InlineKeyboardButton(
            text="🌤 По времени дня", callback_data=f"schedule:{SCHEDULE_TYPE_PERIOD}"
        ),
    )
    return keyboard


def build_day_period_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for preset in DAY_PERIOD_PRESETS:
        keyboard.insert(
            types.InlineKeyboardButton(
                text=preset["title"], callback_data=f"period:{preset['key']}"
            )
        )
    return keyboard


def build_add_more_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(text="Добавить ещё", callback_data="add_more:yes"),
        types.InlineKeyboardButton(text="Хватит", callback_data="add_more:no"),
    )
    return keyboard


def build_language_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    for code, title in LANGUAGE_OPTIONS.items():
        keyboard.insert(
            types.InlineKeyboardButton(text=title, callback_data=f"lang:set:{code}")
        )
    return keyboard


def get_main_reply_keyboard(has_medications: bool):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if has_medications:
        keyboard.add(types.KeyboardButton(MANUAL_MARK_BUTTON))
    keyboard.add(types.KeyboardButton(ADD_MED_BUTTON))
    return keyboard


def normalize_time_input(raw: str) -> Optional[str]:
    if not raw:
        return None
    clean = raw.strip().lower().replace(" ", "")
    clean = clean.replace(".", ":")
    if ":" not in clean and clean.isdigit():
        if len(clean) == 4:
            clean = f"{clean[:2]}:{clean[2:]}"
        elif len(clean) == 3:
            clean = f"0{clean[0]}:{clean[1:]}"
        elif len(clean) == 2:
            clean = f"{clean}:00"
    try:
        parsed = datetime.strptime(clean, "%H:%M")
    except ValueError:
        return None
    return parsed.strftime("%H:%M")


def build_intake_action_keyboard(intake_id: int):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(
            text="✅ Выпил(а)", callback_data=f"take:{intake_id}"
        ),
        types.InlineKeyboardButton(
            text="⏰ Напомни позже", callback_data=f"snooze:{intake_id}"
        ),
    )
    keyboard.add(
        types.InlineKeyboardButton(
            text="🚫 Не напоминать", callback_data=f"skip:{intake_id}"
        )
    )
    return keyboard


# ----------------- DB helpers -----------------
def get_or_create_user(telegram_id: int, first_name: str = None):
    user = db_query(
        "SELECT * FROM users WHERE telegram_id = %s",
        (telegram_id,),
        fetchone=True,
    )
    if user:
        return user

    db_query(
        """
        INSERT INTO users (telegram_id, first_name, timezone, language, remind_before_minutes)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (telegram_id, first_name, DEFAULT_TZ_NAME, "ru", REMIND_BEFORE_MINUTES),
    )
    user = db_query(
        "SELECT * FROM users WHERE telegram_id = %s",
        (telegram_id,),
        fetchone=True,
    )
    return user


def add_medication(
    user_id: int,
    name: str,
    times_list,
    *,
    schedule_mode: str = SCHEDULE_TYPE_EXACT,
    periods: Optional[list[str]] = None,
    doses_per_day: Optional[int] = None,
):
    # times_list: список строк 'HH:MM'
    doses = doses_per_day or len(times_list)
    db_query(
        """
        INSERT INTO medications (user_id, name, times, schedule_mode, periods, doses_per_day)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (user_id, name, times_list, schedule_mode, periods, doses),
    )


def get_user_medications(user_id: int, include_inactive: bool = False):
    base_query = "SELECT * FROM medications WHERE user_id = %s"
    params = [user_id]
    if not include_inactive:
        base_query += " AND is_active = TRUE"
    base_query += " ORDER BY id"
    return db_query(base_query, params, fetchall=True)


def ensure_intake_record(
    med_id: int, scheduled_dt: datetime, default_reminder_at: Optional[datetime]
):
    # Создаём запись приёма, если её нет
    inserted = db_query(
        """
        INSERT INTO intakes (medication_id, scheduled_at, next_reminder_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (medication_id, scheduled_at) DO NOTHING
        RETURNING *
        """,
        (med_id, scheduled_dt, default_reminder_at),
        fetchone=True,
    )
    if inserted:
        return inserted

    row = db_query(
        "SELECT * FROM intakes WHERE medication_id = %s AND scheduled_at = %s",
        (med_id, scheduled_dt),
        fetchone=True,
    )
    if (
        row
        and not row["taken"]
        and not row["reminders_paused"]
        and not row["reminder_sent"]
        and not row["next_reminder_at"]
        and default_reminder_at
    ):
        db_query(
            "UPDATE intakes SET next_reminder_at = %s WHERE id = %s",
            (default_reminder_at, row["id"]),
        )
        row["next_reminder_at"] = default_reminder_at
    return row


def mark_intake_taken(intake_id: int):
    db_query(
        """
        UPDATE intakes
           SET taken = TRUE,
               taken_at = NOW(),
               next_reminder_at = NULL
         WHERE id = %s
        """,
        (intake_id,),
    )


def snooze_intake(intake_id: int, minutes: int = SNOOZE_MINUTES):
    next_time = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    db_query(
        """
        UPDATE intakes
           SET next_reminder_at = %s,
               reminders_paused = FALSE
         WHERE id = %s
        """,
        (next_time, intake_id),
    )


def pause_intake_reminders(intake_id: int):
    db_query(
        """
        UPDATE intakes
           SET reminders_paused = TRUE,
               next_reminder_at = NULL
         WHERE id = %s
        """,
        (intake_id,),
    )


def clear_future_intakes(med_id: int, from_dt: Optional[datetime] = None):
    cutoff = from_dt or datetime.now(timezone.utc)
    db_query(
        """
        DELETE FROM intakes
         WHERE medication_id = %s
           AND scheduled_at >= %s
           AND taken = FALSE
        """,
        (med_id, cutoff),
    )


def set_medication_active(med_id: int, user_id: int, active: bool):
    db_query(
        "UPDATE medications SET is_active = %s WHERE id = %s AND user_id = %s",
        (active, med_id, user_id),
    )


def get_intakes_for_day(med_id: int, target_date: date, zone: ZoneInfo):
    start_utc, end_utc = get_day_bounds_utc(target_date, zone)
    rows = db_query(
        """
        SELECT * FROM intakes
         WHERE medication_id = %s
           AND scheduled_at >= %s
           AND scheduled_at < %s
         ORDER BY scheduled_at
        """,
        (med_id, start_utc, end_utc),
        fetchall=True,
    )
    return rows


def get_user_by_telegram(telegram_id: int):
    return db_query(
        "SELECT * FROM users WHERE telegram_id = %s",
        (telegram_id,),
        fetchone=True,
    )


def get_user_by_id(user_id: int):
    return db_query(
        "SELECT * FROM users WHERE id = %s",
        (user_id,),
        fetchone=True,
    )


def update_user_timezone(user_id: int, tz_name: str):
    db_query(
        "UPDATE users SET timezone = %s WHERE id = %s",
        (tz_name, user_id),
    )


def update_user_language(user_id: int, lang_code: str):
    db_query(
        "UPDATE users SET language = %s WHERE id = %s",
        (lang_code, user_id),
    )


def update_user_remind_before(user_id: int, minutes: int):
    minutes = max(1, min(180, minutes))
    db_query(
        "UPDATE users SET remind_before_minutes = %s WHERE id = %s",
        (minutes, user_id),
    )


def update_medication_schedule(
    med_id: int,
    *,
    times,
    schedule_mode: str,
    periods,
    doses_per_day: int,
):
    db_query(
        """
        UPDATE medications
           SET times = %s,
               schedule_mode = %s,
               periods = %s,
               doses_per_day = %s
         WHERE id = %s
        """,
        (times, schedule_mode, periods, doses_per_day, med_id),
    )


def delete_medication(med_id: int, user_id: int):
    db_query(
        "DELETE FROM medications WHERE id = %s AND user_id = %s",
        (med_id, user_id),
    )


def get_med_by_id(med_id: int):
    return db_query(
        "SELECT * FROM medications WHERE id = %s",
        (med_id,),
        fetchone=True,
    )


def get_all_users():
    return db_query("SELECT * FROM users", fetchall=True) or []


# ----------------- Клавиатуры -----------------
def build_today_progress_keyboard(med, intakes, zone: ZoneInfo):
    """
    Для inline-кнопки отметки приёма (используем только на напоминаниях)
    """
    keyboard = types.InlineKeyboardMarkup()
    for intake in intakes:
        time_str = to_local(intake["scheduled_at"], zone).strftime("%H:%M")
        status = "✅" if intake["taken"] else "❌"
        keyboard.add(
            types.InlineKeyboardButton(
                text=f"{time_str} {status}",
                callback_data=f"take:{intake['id']}",
            )
        )
    return keyboard


# ----------------- Команды бота -----------------
@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    user = get_or_create_user(message.from_user.id, message.from_user.first_name)
    meds = get_user_medications(user["id"])
    has_meds = bool(meds)
    text = (
        "Привет! Я помогу не забывать пить таблетки 💊\n\n"
        "Команды:\n"
        "/add — добавить лекарство\n"
        "/list — показать список лекарств и прогресс за сегодня\n"
        "/help — подсказка по возможностям\n"
        "/meds — управление лекарствами\n"
        "/timezone — сменить часовой пояс\n\n"
        f"Когда всё настроишь — пользуйся кнопкой \"{MANUAL_MARK_BUTTON}\" внизу, "
        "если отметила приём заранее."
    )
    await message.answer(text, reply_markup=get_main_reply_keyboard(has_meds))
    if not has_meds:
        await message.answer("Давай настроим первое лекарство.")
        await cmd_add(message, state)


@dp.message_handler(commands=["add"], state="*")
async def cmd_add(message: types.Message, state: FSMContext):
    get_or_create_user(message.from_user.id, message.from_user.first_name)
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    await AddMedStates.waiting_for_name.set()
    await state.update_data(times=[], periods=[])
    await message.answer("Как называется лекарство?")


@dp.message_handler(lambda message: message.text == ADD_MED_BUTTON, state="*")
async def handle_add_button(message: types.Message, state: FSMContext):
    await cmd_add(message, state)


@dp.message_handler(lambda message: message.text == MANUAL_MARK_BUTTON, state="*")
async def handle_manual_mark_button(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state and current_state.startswith("AddMedStates"):
        await message.answer("Сначала закончи настройку лекарства.")
        return

    user = get_user_by_telegram(message.from_user.id)
    if not user:
        await message.answer("Сначала отправь /start")
        return

    meds = get_user_medications(user["id"])
    if not meds:
        await message.answer("Список лекарств пуст. Используй /add.")
        return

    zone = get_zone_for_user(user)
    today_local = get_local_today(zone)
    sent = False
    for med in meds:
        intakes = get_intakes_for_day(med["id"], today_local, zone)
        if not any(not intake["taken"] for intake in intakes):
            continue
        keyboard = build_today_progress_keyboard(med, intakes, zone)
        await message.answer(
            f"💊 {med['name']}\nВыбери приём, который уже выполнен:",
            reply_markup=keyboard,
        )
        sent = True

    if not sent:
        await message.answer("На сегодня всё отмечено 🙌")


@dp.message_handler(state=AddMedStates.waiting_for_name)
async def add_med_name(message: types.Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Пожалуйста, введи название лекарства.")
        return
    await state.update_data(name=name)
    await message.answer("Сколько раз в день его принимать? (цифра от 1 до 8)")
    await AddMedStates.waiting_for_times_per_day.set()


@dp.message_handler(state=AddMedStates.waiting_for_times_per_day)
async def add_med_times_per_day(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Нужна цифра, например 2.")
        return
    count = int(raw)
    if count <= 0 or count > 10:
        await message.answer("Давай выберем что-то в пределах 1-10.")
        return
    await state.update_data(dose_count=count, times=[], periods=[])
    await message.answer(
        "Как удобнее задать расписание? Выбери вариант:",
        reply_markup=build_schedule_type_keyboard(),
    )
    await AddMedStates.waiting_for_schedule_type.set()


@dp.callback_query_handler(
    lambda call: call.data.startswith("schedule:"), state=AddMedStates.waiting_for_schedule_type
)
async def add_med_schedule_type(call: types.CallbackQuery, state: FSMContext):
    _, mode = call.data.split(":")
    if mode not in (SCHEDULE_TYPE_EXACT, SCHEDULE_TYPE_PERIOD):
        await call.answer("Неизвестный вариант", show_alert=True)
        return
    await state.update_data(schedule_mode=mode)
    data = await state.get_data()
    if mode == SCHEDULE_TYPE_EXACT:
        await AddMedStates.waiting_for_exact_time.set()
        await call.message.answer(
            f"Введи время #1 из {data['dose_count']} (например, 09:00)"
        )
    else:
        await AddMedStates.waiting_for_day_period.set()
        await call.message.answer(
            "Выбери отрезок времени для приёма:",
            reply_markup=build_day_period_keyboard(),
        )
    await call.answer()


@dp.message_handler(state=AddMedStates.waiting_for_exact_time)
async def add_exact_time(message: types.Message, state: FSMContext):
    data = await state.get_data()
    times = data.get("times", [])
    normalized = normalize_time_input(message.text or "")
    if not normalized:
        await message.answer("Нужен формат HH:MM, например 08:30.")
        return
    times.append(normalized)
    await state.update_data(times=times)
    if len(times) < data["dose_count"]:
        await message.answer(
            f"Отлично! Введи время #{len(times) + 1} из {data['dose_count']}."
        )
    else:
        await finalize_medication_entry(message, state)


@dp.callback_query_handler(
    lambda call: call.data.startswith("period:"), state=AddMedStates.waiting_for_day_period
)
async def add_period_time(call: types.CallbackQuery, state: FSMContext):
    _, key = call.data.split(":")
    preset = get_period_by_key(key)
    if not preset:
        await call.answer("Неизвестный отрезок", show_alert=True)
        return
    data = await state.get_data()
    periods = data.get("periods", [])
    times = data.get("times", [])
    periods.append(key)
    times.append(preset["time"].strftime("%H:%M"))
    await state.update_data(periods=periods, times=times)
    await call.answer(f"Добавлено: {preset['title']}")
    remaining = data["dose_count"] - len(times)
    if remaining > 0:
        await call.message.answer(
            f"Осталось выбрать {remaining}.", reply_markup=build_day_period_keyboard()
        )
    else:
        await finalize_medication_entry(call.message, state)


async def finalize_medication_entry(source_message: types.Message, state: FSMContext):
    data = await state.get_data()
    name = data["name"]
    times = data.get("times", [])
    periods = data.get("periods", []) if data.get("schedule_mode") == SCHEDULE_TYPE_PERIOD else None
    dose_count = data.get("dose_count", len(times))
    edit_med_id = data.get("edit_med_id")
    user = get_or_create_user(
        source_message.from_user.id, source_message.from_user.first_name
    )

    schedule_mode = data.get("schedule_mode", SCHEDULE_TYPE_EXACT)
    if edit_med_id:
        update_medication_schedule(
            edit_med_id,
            times=times,
            schedule_mode=schedule_mode,
            periods=periods,
            doses_per_day=dose_count,
        )
        clear_future_intakes(edit_med_id)
    else:
        add_medication(
            user["id"],
            name,
            times,
            schedule_mode=schedule_mode,
            periods=periods,
            doses_per_day=dose_count,
        )
    pretty_times = []
    if periods:
        for idx, t in enumerate(times):
            label = periods[idx] if idx < len(periods) else None
            pretty_times.append(format_period_label(label, t))
    else:
        pretty_times = times
    summary = (
        f"Лекарство *{name}* {'обновлено' if edit_med_id else 'добавлено'}.\n"
        f"Расписание: {', '.join(pretty_times)}"
    )
    await source_message.answer(summary, parse_mode="Markdown")

    if edit_med_id:
        await state.finish()
        has_meds = bool(get_user_medications(user["id"]))
        await source_message.answer(
            "Готово! Если нужно, открой список через /meds.",
            reply_markup=get_main_reply_keyboard(has_meds),
        )
        return

    await AddMedStates.waiting_for_remind_before.set()
    await source_message.answer(
        "За сколько минут заранее напоминать? (1-180)",
    )


@dp.message_handler(state=AddMedStates.waiting_for_remind_before)
async def set_remind_before(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user = get_or_create_user(message.from_user.id, message.from_user.first_name)
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer("Нужна цифра (минуты). Например: 10")
        return
    minutes = max(1, min(180, int(raw)))
    update_user_remind_before(user["id"], minutes)
    await message.answer(f"Буду напоминать за {minutes} минут до приёма 💊")

    await state.update_data(times=[], periods=[], edit_med_id=None)
    await AddMedStates.confirming_more.set()
    await source_message.answer(
        "Добавим ещё лекарство?", reply_markup=build_add_more_keyboard()
    )


@dp.callback_query_handler(
    lambda call: call.data.startswith("add_more:"), state=AddMedStates.confirming_more
)
async def add_more_medications(call: types.CallbackQuery, state: FSMContext):
    _, choice = call.data.split(":")
    user = get_or_create_user(call.from_user.id, call.from_user.first_name)
    meds = get_user_medications(user["id"])
    has_meds = bool(meds)

    await call.answer()
    await call.message.edit_reply_markup()

    if choice == "yes":
        await AddMedStates.waiting_for_name.set()
        await state.update_data(times=[], periods=[], name=None, dose_count=None, schedule_mode=None)
        await call.message.answer("Отлично! Как называется следующее лекарство?")
        return

    await state.finish()
    await call.message.answer(
        "Супер! Напоминания включены.",
        reply_markup=get_main_reply_keyboard(has_meds),
    )


@dp.message_handler(commands=["list"])
async def cmd_list(message: types.Message):
    user = get_user_by_telegram(message.from_user.id)
    if not user:
        await message.answer("Сначала отправь /start")
        return

    meds = get_user_medications(user["id"])
    if not meds:
        await message.answer(
            "У тебя пока нет добавленных лекарств. Добавь через /add.",
            reply_markup=get_main_reply_keyboard(False),
        )
        return

    lines = []
    zone = get_zone_for_user(user)
    today_local = get_local_today(zone)
    now_utc = datetime.now(timezone.utc)
    for med in meds:
        intakes = get_intakes_for_day(med["id"], today_local, zone)
        total = len(intakes)
        taken = sum(1 for i in intakes if i["taken"])
        times_str = format_med_schedule(med)
        overdue = sum(
            1 for i in intakes if not i["taken"] and i["scheduled_at"] < now_utc
        )
        lines.append(
            f"💊 *{med['name']}*\n"
            f"Время: {times_str}\n"
            f"Сегодня: {taken}/{total}"
        )
        if overdue:
            lines.append(f"Просрочено: {overdue}")
        lines.append("")

    lines.append(f"Часовой пояс: {user.get('timezone') or DEFAULT_TZ_NAME}")
    await message.answer(
        "\n".join(line for line in lines if line),
        parse_mode="Markdown",
        reply_markup=get_main_reply_keyboard(True),
    )


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    user = get_or_create_user(message.from_user.id, message.from_user.first_name)
    lang = get_language_for_user(user)
    text = HELP_TEXTS.get(lang, HELP_TEXTS["ru"])
    meds = get_user_medications(user["id"])
    await message.answer(
        text,
        reply_markup=get_main_reply_keyboard(bool(meds)),
    )


@dp.message_handler(commands=["meds", "manage"])
async def cmd_manage_meds(message: types.Message):
    user = get_user_by_telegram(message.from_user.id)
    if not user:
        await message.answer("Сначала отправь /start")
        return

    meds = get_user_medications(user["id"], include_inactive=True)
    if not meds:
        await message.answer(
            "Список пуст. Используй /add, чтобы добавить лекарство.",
            reply_markup=get_main_reply_keyboard(False),
        )
        return

    zone = get_zone_for_user(user)
    today_local = get_local_today(zone)
    for med in meds:
        intakes = get_intakes_for_day(med["id"], today_local, zone)
        total = len(intakes)
        taken = sum(1 for i in intakes if i["taken"])
        status_text = "🟢 Активно" if med["is_active"] else "⏸ На паузе"
        text = (
            f"💊 *{med['name']}*\n"
            f"Расписание: {format_med_schedule(med)}\n"
            f"Сегодня: {taken}/{total}\n"
            f"Статус: {status_text}"
        )
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton(
                text="📝 Изменить", callback_data=f"med:edit:{med['id']}"
            ),
            types.InlineKeyboardButton(
                text="🗑 Удалить", callback_data=f"med:delete:{med['id']}"
            ),
        )
        toggle_label = "⏸ Пауза" if med["is_active"] else "▶️ Возобновить"
        keyboard.add(
            types.InlineKeyboardButton(
                text=toggle_label, callback_data=f"med:toggle:{med['id']}"
            )
        )
        await message.answer(
            text, parse_mode="Markdown", reply_markup=keyboard
        )


@dp.message_handler(commands=["timezone"], state="*")
async def cmd_timezone(message: types.Message):
    user = get_or_create_user(message.from_user.id, message.from_user.first_name)
    has_meds = bool(get_user_medications(user["id"]))

    args = message.get_args()
    if not args:
        await message.answer(
            "Текущий часовой пояс: {tz}\n"
            "Чтобы изменить, введи команду в формате:\n"
            "`/timezone Europe/Moscow`".format(
                tz=user.get("timezone") or DEFAULT_TZ_NAME
            ),
            parse_mode="Markdown",
            reply_markup=get_main_reply_keyboard(has_meds),
        )
        return

    tz_name = args.strip()
    if not is_valid_timezone(tz_name):
        await message.answer(
            "Не могу найти такой часовой пояс. Пример: Europe/Moscow",
            reply_markup=get_main_reply_keyboard(has_meds),
        )
        return

    update_user_timezone(user["id"], tz_name)
    await message.answer(
        f"Готово! Теперь часовой пояс: {tz_name}",
        reply_markup=get_main_reply_keyboard(has_meds),
    )


@dp.message_handler(commands=["remind"], state="*")
async def cmd_remind(message: types.Message):
    user = get_or_create_user(message.from_user.id, message.from_user.first_name)
    has_meds = bool(get_user_medications(user["id"]))

    args = (message.get_args() or "").strip()
    if not args:
        await message.answer(
            f"Сейчас напоминаю за {get_remind_before(user)} минут.\n"
            "Введи `/remind 15`, чтобы изменить.",
            parse_mode="Markdown",
            reply_markup=get_main_reply_keyboard(has_meds),
        )
        return

    if not args.isdigit():
        await message.answer(
            "Нужна цифра в минутах, например `/remind 20`.",
            parse_mode="Markdown",
            reply_markup=get_main_reply_keyboard(has_meds),
        )
        return

    minutes = max(1, min(180, int(args)))
    update_user_remind_before(user["id"], minutes)
    await message.answer(
        f"Буду напоминать за {minutes} минут.",
        reply_markup=get_main_reply_keyboard(has_meds),
    )


@dp.message_handler(commands=["language"], state="*")
async def cmd_language(message: types.Message):
    user = get_or_create_user(message.from_user.id, message.from_user.first_name)
    has_meds = bool(get_user_medications(user["id"]))

    args = (message.get_args() or "").strip().lower()
    if args and args in LANGUAGE_OPTIONS:
        update_user_language(user["id"], args)
        await message.answer(
            f"Готово! Выбрали язык: {LANGUAGE_OPTIONS[args]}",
            reply_markup=get_main_reply_keyboard(has_meds),
        )
        return

    current = get_language_for_user(user)
    await message.answer(
        f"Текущий язык: {LANGUAGE_OPTIONS.get(current, 'Русский')}\n"
        "Выбери новый:",
        reply_markup=build_language_keyboard(),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("lang:set:"))
async def callback_set_language(call: types.CallbackQuery):
    _, _, lang_code = call.data.split(":")
    user = get_or_create_user(call.from_user.id, call.from_user.first_name)
    if lang_code not in LANGUAGE_OPTIONS:
        await call.answer("Неизвестный язык", show_alert=True)
        return
    update_user_language(user["id"], lang_code)
    await call.answer("Готово!")
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(
        f"Теперь язык: {LANGUAGE_OPTIONS[lang_code]}",
        reply_markup=get_main_reply_keyboard(bool(get_user_medications(user["id"]))),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("med:edit:"))
async def callback_edit_medication(call: types.CallbackQuery, state: FSMContext):
    _, _, med_id_str = call.data.split(":")
    med_id = int(med_id_str)

    user = get_user_by_telegram(call.from_user.id)
    if not user:
        await call.answer("Сначала отправь /start", show_alert=True)
        return

    med = get_med_by_id(med_id)
    if not med or med["user_id"] != user["id"]:
        await call.answer("Лекарство не найдено.", show_alert=True)
        return

    await state.finish()
    await AddMedStates.waiting_for_times_per_day.set()
    await state.update_data(
        name=med["name"],
        edit_med_id=med_id,
        times=[],
        periods=[],
    )
    await call.answer()
    await call.message.answer(
        f"Обновим расписание для *{med['name']}*.\n"
        f"Сколько раз в день принимать? (сейчас {med.get('doses_per_day', len(med.get('times', [])))} раз)",
        parse_mode="Markdown",
    )


@dp.callback_query_handler(lambda c: c.data.startswith("med:delete:"))
async def callback_delete_medication(call: types.CallbackQuery):
    _, _, med_id_str = call.data.split(":")
    med_id = int(med_id_str)

    user = get_user_by_telegram(call.from_user.id)
    if not user:
        await call.answer("Сначала отправь /start", show_alert=True)
        return

    med = get_med_by_id(med_id)
    if not med or med["user_id"] != user["id"]:
        await call.answer("Лекарство не найдено.", show_alert=True)
        return

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(
            text="✅ Да, удалить", callback_data=f"med:delete_confirm:{med_id}:yes"
        ),
        types.InlineKeyboardButton(
            text="↩️ Оставить", callback_data=f"med:delete_confirm:{med_id}:no"
        ),
    )
    await call.answer()
    await call.message.answer(
        f"Точно удалить *{med['name']}*?",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


@dp.callback_query_handler(lambda c: c.data.startswith("med:delete_confirm:"))
async def callback_delete_confirm(call: types.CallbackQuery):
    _, _, med_id_str, choice = call.data.split(":")
    med_id = int(med_id_str)
    user = get_user_by_telegram(call.from_user.id)
    if not user:
        await call.answer("Сначала отправь /start", show_alert=True)
        return

    med = get_med_by_id(med_id)
    if not med or med["user_id"] != user["id"]:
        await call.answer("Лекарство не найдено.", show_alert=True)
        return

    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)

    if choice == "no":
        await call.message.answer("Оставили без изменений.")
        return

    delete_medication(med_id, user["id"])
    logger.info(
        "Medication deleted", extra={"med_id": med_id, "user_id": user["id"]}
    )
    has_meds = bool(get_user_medications(user["id"]))
    await call.message.answer(
        f"Удалил *{med['name']}*.",
        parse_mode="Markdown",
        reply_markup=get_main_reply_keyboard(has_meds),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("med:toggle:"))
async def callback_toggle_medication(call: types.CallbackQuery):
    _, _, med_id_str = call.data.split(":")
    med_id = int(med_id_str)

    user = get_user_by_telegram(call.from_user.id)
    if not user:
        await call.answer("Сначала отправь /start", show_alert=True)
        return

    med = get_med_by_id(med_id)
    if not med or med["user_id"] != user["id"]:
        await call.answer("Лекарство не найдено.", show_alert=True)
        return

    new_state = not med["is_active"]
    set_medication_active(med_id, user["id"], new_state)
    if new_state:
        clear_future_intakes(med_id)
        logger.info("Medication resumed", extra={"med_id": med_id, "user_id": user["id"]})
        msg = f"Возобновил напоминания для *{med['name']}*."
    else:
        logger.info("Medication paused", extra={"med_id": med_id, "user_id": user["id"]})
        msg = f"Поставил *{med['name']}* на паузу."

    await call.answer()
    await call.message.answer(
        msg,
        parse_mode="Markdown",
        reply_markup=get_main_reply_keyboard(
            bool(get_user_medications(user["id"]))
        ),
    )


@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not ADMIN_CHAT_ID or message.from_user.id != ADMIN_CHAT_ID:
        await message.answer("Команда доступна только администратору.")
        return

    lines = [
        "📊 Метрики с момента запуска:",
        f"- Отправлено напоминаний: {METRICS['reminders_sent']}",
        f"- Ошибок отправки: {METRICS['reminders_failed']}",
        f"- Сноузов: {METRICS['snoozes']}",
        f"- Отметок приёма: {METRICS['intakes_marked']}",
        f"- Пропущено: {METRICS['missed']}",
    ]
    await message.answer("\n".join(lines))


@dp.message_handler(commands=["daily"])
async def cmd_daily(message: types.Message):
    user = get_user_by_telegram(message.from_user.id)
    if not user:
        await message.answer("Сначала отправь /start")
        return

    sent = await send_summary_for_user(user)
    if not sent:
        await message.answer("На сегодня нет данных или всё ещё впереди ✨")
# ----------------- Callback для отметки приёма -----------------
@dp.callback_query_handler(
    lambda c: c.data.startswith(("take:", "snooze:", "skip:"))
)
async def callback_intake_actions(call: types.CallbackQuery):
    try:
        action, intake_id_str = call.data.split(":", 1)
        intake_id = int(intake_id_str)
    except (ValueError, IndexError):
        await call.answer("Неверные данные.", show_alert=True)
        return

    intake = db_query(
        "SELECT * FROM intakes WHERE id = %s",
        (intake_id,),
        fetchone=True,
    )
    if not intake:
        await call.answer("Запись не найдена.", show_alert=True)
        return

    med = get_med_by_id(intake["medication_id"])
    if not med:
        await call.answer("Лекарство не найдено.", show_alert=True)
        return

    user = get_user_by_id(med["user_id"])
    if not user:
        await call.answer("Пользователь не найден.", show_alert=True)
        return

    zone = get_zone_for_user(user)
    today_local = get_local_today(zone)

    if action == "take":
        if intake["taken"]:
            await call.answer("Уже отмечено как принял(а) ✅", show_alert=True)
            return
        mark_intake_taken(intake_id)
        METRICS["intakes_marked"] += 1
        await call.answer("Отметил как принял(а) ✅", show_alert=True)
        intakes_today = get_intakes_for_day(med["id"], today_local, zone)
        if call.message and call.message.text and "Выбери приём" in call.message.text:
            keyboard = build_today_progress_keyboard(med, intakes_today, zone)
            await call.message.edit_reply_markup(reply_markup=keyboard)
        else:
            await call.message.edit_reply_markup(reply_markup=None)
        return

    if action == "snooze":
        if intake["taken"]:
            await call.answer("Приём уже отмечен.", show_alert=True)
            return
        snooze_intake(intake_id)
        METRICS["snoozes"] += 1
        await call.answer("Напомню через 15 минут ⏰")
        return

    if action == "skip":
        pause_intake_reminders(intake_id)
        await call.answer("Напоминания для этого приёма отключены.", show_alert=True)
        await call.message.edit_reply_markup(reply_markup=None)


# ----------------- Фоновые напоминания -----------------
async def reminder_loop():
    """
    Простая петля: раз в минуту проверяем, какие приёмы пора напомнить.
    """
    while True:
        try:
            await check_and_send_reminders()
        except Exception as e:
            logger.exception("Error in reminder_loop: %s", e)
            await notify_admin("Фоновый цикл напоминаний упал")
        await asyncio.sleep(60)


async def check_and_send_reminders():
    now_utc = datetime.now(timezone.utc)

    # Берём все активные лекарства
    meds = db_query(
        "SELECT * FROM medications WHERE is_active = TRUE",
        fetchall=True,
    )
    if not meds:
        return

    for med in meds:
        user = get_user_by_id(med["user_id"])
        if not user:
            continue
        zone = get_zone_for_user(user)
        remind_before = get_remind_before(user)
        today_local = get_local_today(zone)

        for day_offset in range(0, 2):
            target_date = today_local + timedelta(days=day_offset)

            for t_str in med["times"] or []:
                scheduled_dt = local_time_to_utc(target_date, t_str, zone)
                default_reminder_at = scheduled_dt - timedelta(
                    minutes=remind_before
                )
                if day_offset == 0 and default_reminder_at < now_utc:
                    default_reminder_at = now_utc

                intake = ensure_intake_record(
                    med["id"], scheduled_dt, default_reminder_at
                )
                if day_offset > 0 or not intake or intake["taken"] or intake["reminders_paused"]:
                    continue

                next_reminder_at = intake["next_reminder_at"]
                if not next_reminder_at:
                    continue

                if now_utc >= next_reminder_at:
                    local_time_str = to_local(
                        intake["scheduled_at"], zone
                    ).strftime("%H:%M")
                    text = (
                        f"💊 Скоро приём *{med['name']}* ({local_time_str}).\n"
                        "Как только выпьешь — нажми на кнопку."
                    )
                    keyboard = build_intake_action_keyboard(intake["id"])

                    try:
                        await bot.send_message(
                            chat_id=user["telegram_id"],
                            text=text,
                            parse_mode="Markdown",
                            reply_markup=keyboard,
                        )
                        db_query(
                            """
                            UPDATE intakes
                               SET reminder_sent = TRUE,
                                   last_reminder_at = %s,
                                   next_reminder_at = NULL
                             WHERE id = %s
                            """,
                            (now_utc, intake["id"]),
                        )
                        METRICS["reminders_sent"] += 1
                        logger.info(
                            "Reminder sent",
                            extra={
                                "med_id": med["id"],
                                "intake_id": intake["id"],
                                "user_id": user["id"],
                            },
                        )
                    except Exception as e:
                        METRICS["reminders_failed"] += 1
                        logger.exception("Failed to send reminder: %s", e)
                        await notify_admin(f"Не смог отправить напоминание пользователю {user['id']}")


async def send_summary_for_user(user: dict) -> bool:
    zone = get_zone_for_user(user)
    today = get_local_today(zone)
    meds = get_user_medications(user["id"])
    if not meds:
        return False

    lines = [f"📅 Итоги за {today.strftime('%d.%m')}:"]
    has_data = False
    for med in meds:
        intakes = get_intakes_for_day(med["id"], today, zone)
        total = len(intakes)
        if total == 0:
            continue
        taken = sum(1 for i in intakes if i["taken"])
        missed = total - taken
        if missed:
            METRICS["missed"] += missed
        line = f"- {med['name']}: {taken}/{total}"
        if missed:
            line += f" (пропущено {missed})"
        lines.append(line)
        has_data = True

    if not has_data:
        return False

    try:
        await bot.send_message(
            chat_id=user["telegram_id"],
            text="\n".join(lines),
        )
        return True
    except Exception as exc:
        logger.exception("Failed to send summary: %s", exc)
        await notify_admin(f"Не смог отправить дневной отчёт пользователю {user['id']}")
        return False


async def maybe_send_daily_summary():
    users = get_all_users()
    for user in users:
        zone = get_zone_for_user(user)
        local_now = datetime.now(zone)
        today = local_now.date()
        if local_now.hour < SUMMARY_HOUR:
            continue
        if SUMMARY_STATE.get(user["id"]) == today:
            continue

        sent = await send_summary_for_user(user)
        if sent:
            SUMMARY_STATE[user["id"]] = today


async def daily_summary_loop():
    while True:
        try:
            await maybe_send_daily_summary()
        except Exception as exc:
            logger.exception("Error in daily_summary_loop: %s", exc)
            await notify_admin("Фоновый цикл ежедневных отчётов упал")
        await asyncio.sleep(60)


# ----------------- Запуск -----------------
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(reminder_loop())
    loop.create_task(daily_summary_loop())
    executor.start_polling(dp, skip_updates=True)
