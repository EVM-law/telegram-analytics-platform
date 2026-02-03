"""
Telegram-бот для генерации аналитических отчетов по Telegram-каналам.
Интегрируется с FastAPI сервисом Telegram Analytics Platform.

Архитектура:
- FSM (Finite State Machine) для управления диалогом
- Асинхронная обработка с защитой от повторных запросов
- Фоновые задачи для долгих операций
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Optional, Tuple, Set
from time import monotonic

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.exceptions import TelegramBadRequest

# ============================================================================
# КОНФИГУРАЦИЯ (из переменных окружения Amvera)
# ============================================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# На Amvera uvicorn запущен на порту 80 (из amvera.yaml)
# Бот и API в одном контейнере, поэтому localhost:80
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:80")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "900"))  # увеличено по умолчанию до 15 минут
ALLOWED_USERS = os.getenv("ALLOWED_USERS", "")

# ============================================================================
# ИНИЦИАЛИЗАЦИЯ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)

storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ============================================================================
# TASK MANAGER: Правильное управление фоновыми задачами
# ============================================================================

# Хранилище для предотвращения удаления задач сборщиком мусора
background_tasks: Set[asyncio.Task] = set()

class TaskManager:
    """
    Менеджер задач для правильного управления фоновыми операциями.
    
    Решает проблемы:
    - Предотвращает удаление задач garbage collector'ом
    - Изолирует задачи разных пользователей
    - Автоматическая очистка после завершения
    """
    def __init__(self):
        self._user_tasks: Dict[int, asyncio.Task] = {}

    async def can_start_task(self, user_id: int) -> bool:
        """Проверяет, может ли пользователь запустить новую задачу"""
        existing_task = self._user_tasks.get(user_id)
        if existing_task is None:
            return True

        # Проверяем, завершилась ли предыдущая задача
        if existing_task.done():
            del self._user_tasks[user_id]
            return True

        return False

    def add_task(self, user_id: int, task: asyncio.Task):
        """Добавляет задачу для пользователя с автоматической очисткой"""
        self._user_tasks[user_id] = task

        # Автоматическая очистка при завершении
        def cleanup(t):
            self._user_tasks.pop(user_id, None)
            background_tasks.discard(t)

        task.add_done_callback(cleanup)
        background_tasks.add(task)

    def get_active_users(self) -> list:
        """Возвращает список пользователей с активными задачами"""
        return [uid for uid, task in self._user_tasks.items() if not task.done()]
    
    def get_task_info(self, user_id: int) -> Optional[dict]:
        """Получает информацию о задаче пользователя"""
        task = self._user_tasks.get(user_id)
        if task is None:
            return None
        
        return {
            "user_id": user_id,
            "done": task.done(),
            "cancelled": task.cancelled()
        }

# Создаем глобальный TaskManager
task_manager = TaskManager()


# ============================================================================
# FSM СОСТОЯНИЯ
# ============================================================================

class ReportStates(StatesGroup):
    """Состояния процесса генерации отчета"""
    choosing_provider_mode = State()  # NEW: Free/Paid выбор провайдера
    choosing_channel_source = State()
    choosing_report_type = State()
    choosing_period = State()
    entering_days = State()
    choosing_model = State()


# ============================================================================
# УТИЛИТЫ
# ============================================================================

def parse_allowed_users() -> set:
    """Парсинг списка разрешенных пользователей из переменной окружения"""
    if not ALLOWED_USERS:
        return set()
    try:
        return set(int(uid.strip()) for uid in ALLOWED_USERS.split(",") if uid.strip())
    except ValueError:
        logger.error("Ошибка парсинга ALLOWED_USERS. Проверьте формат.")
        return set()


def is_user_allowed(user_id: int) -> bool:
    """Проверка доступа пользователя"""
    allowed = parse_allowed_users()
    if not allowed:  # Если список пуст, разрешаем всем
        return True
    return user_id in allowed


def get_admin_user_id() -> int | None:
    """
    Возвращает ID администратора (первый пользователь из ALLOWED_USERS).
    
    Администратор используется для:
    - Вывода сервисных сообщений об ошибках
    - Расширенного доступа к функциям (если требуется)
    
    Если ALLOWED_USERS пустой — администратор не определён (возвращает None).
    """
    allowed = parse_allowed_users()
    if allowed:
        # Возвращаем первый ID из списка (по порядку в строке ALLOWED_USERS)
        # Поскольку set не сохраняет порядок, парсим заново
        try:
            first_id = int(ALLOWED_USERS.split(",")[0].strip())
            return first_id
        except (ValueError, IndexError):
            return None
    return None


# ==========================================================================
# SAFE TELEGRAM OPS (подавление бенIGN TelegramBadRequest для стабильного async)
# ==========================================================================


async def safe_call(coro, op_desc: str = "telegram_op"):
    try:
        return await coro
    except TelegramBadRequest as e:
        # Игнорируем частые безобидные ошибки Telegram API
        msg = str(e).lower()
        benign = (
            "message is not modified",
            "message to edit not found",
            "message not found",
            "message_id_invalid",
            "query is too old",
            "query id is invalid",
        )
        if any(s in msg for s in benign):
            logger.warning(f"Ignored TelegramBadRequest during {op_desc}: {e}")
            return None
        raise


# Простая защита от флуда: минимальный интервал между действиями пользователя
_last_action_at: Dict[int, float] = {}
RATE_LIMIT_SECONDS = 0.6

def rate_limited(user_id: int) -> bool:
    now = monotonic()
    last = _last_action_at.get(user_id, 0.0)
    if now - last < RATE_LIMIT_SECONDS:
        return True
    _last_action_at[user_id] = now
    return False


# ============================================================================
# КЛАВИАТУРЫ
# ============================================================================

def create_report_type_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора типа отчета"""
    buttons = [
        [InlineKeyboardButton(text="Дайджест новостей", callback_data="type_news")],
        [InlineKeyboardButton(text="Календарь мероприятий", callback_data="type_events")],
        [InlineKeyboardButton(text="Доп. сценарий", callback_data="type_custom_task_1")],
        [InlineKeyboardButton(text="Доп. сценарий 2", callback_data="type_custom_task_2")],
        [InlineKeyboardButton(text="Начать сначала", callback_data="nav_reset")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_period_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора периода парсинга"""
    buttons = [
        [InlineKeyboardButton(text="За сегодня", callback_data="period_today")],
        [InlineKeyboardButton(text="За вчера", callback_data="period_yesterday")],
        [InlineKeyboardButton(text="Указать вручную", callback_data="period_manual")],
        [
            InlineKeyboardButton(text="Назад", callback_data="nav_back"),
            InlineKeyboardButton(text="Начать сначала", callback_data="nav_reset")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_source_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора источника каналов (анонимизированные подборки)"""
    buttons = [
        [InlineKeyboardButton(text="Подборка 1", callback_data="source_source_1")],
        [InlineKeyboardButton(text="Подборка 2", callback_data="source_source_2")],
        [InlineKeyboardButton(text="Подборка 3", callback_data="source_source_3")],
        [InlineKeyboardButton(text="Подборка 4", callback_data="source_source_4")],
        [InlineKeyboardButton(text="Подборка 5", callback_data="source_source_5")],
        [InlineKeyboardButton(text="Подборка 6", callback_data="source_source_6")],
        [
            InlineKeyboardButton(text="Назад", callback_data="nav_back_to_provider"),
            InlineKeyboardButton(text="Начать сначала", callback_data="nav_reset")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для отмены ручного ввода"""
    buttons = [
        [InlineKeyboardButton(text="Отмена", callback_data="nav_reset")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_model_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора модели Gemini"""
    buttons = [
        [InlineKeyboardButton(text="2.5 Flash", callback_data="model_flash_2_5")],
        [InlineKeyboardButton(text="3.0 Flash", callback_data="model_flash_3_0")],
        [
            InlineKeyboardButton(text="Назад", callback_data="nav_back_to_period"),
            InlineKeyboardButton(text="Начать сначала", callback_data="nav_reset")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_restart_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для создания нового отчета после завершения"""
    buttons = [
        [InlineKeyboardButton(text="📊 Создать новый отчет", callback_data="nav_start")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_provider_mode_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора режима провайдера LLM"""
    buttons = [
        [InlineKeyboardButton(text="🆓 Бесплатно (Google)", callback_data="mode_free")],
        [InlineKeyboardButton(text="💳 Платно (Polza.ai)", callback_data="mode_paid")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ============================================================================
# API ФУНКЦИИ
# ============================================================================

async def call_report_api(params: dict) -> Tuple[bytes, str]:
    """
    Асинхронный вызов API для генерации отчета.
    
    Args:
        params: Параметры запроса
            - report_type: str (news/events/custom_task_1/custom_task_2)
            - period: str (today/yesterday) или
            - days: int (количество дней)
    
    Returns:
        Tuple[bytes, str]: (содержимое .docx файла, имя файла)
    
    Raises:
        Exception: При ошибке API или таймауте
    """
    url = f"{API_BASE_URL}/api/reports/parse-and-generate"
    
    async with aiohttp.ClientSession() as session:
        try:
            timeout = aiohttp.ClientTimeout(total=API_TIMEOUT)
            async with session.post(url, json=params, timeout=timeout) as response:
                if response.status == 200:
                    # Извлечение имени файла из заголовков
                    content_disposition = response.headers.get('Content-Disposition', '')
                    filename = "report.docx"
                    
                    if 'filename=' in content_disposition:
                        # Парсинг: filename="report.docx" или filename*=UTF-8''report.docx
                        parts = content_disposition.split('filename=')
                        if len(parts) > 1:
                            filename = parts[1].strip('"').split(';')[0]
                    
                    file_content = await response.read()
                    return file_content, filename
                
                elif response.status == 422:
                    error_data = await response.json()
                    detail = error_data.get("detail", "Неизвестная ошибка валидации")
                    raise Exception(f"Ошибка валидации параметров: {detail}")
                
                else:
                    error_text = await response.text()
                    raise Exception(f"API вернул код {response.status}: {error_text[:200]}")
        
        except asyncio.TimeoutError:
            raise Exception(
                f"API не ответил за {API_TIMEOUT} секунд. "
                "Возможно, слишком много данных для парсинга."
            )
        except aiohttp.ClientError as e:
            raise Exception(f"Ошибка соединения с API: {str(e)}")


# ============================================================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================================================

@dp.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    """Точка входа: приветствие и запуск диалога"""
    user_id = message.from_user.id
    
    # Проверка доступа
    if not is_user_allowed(user_id):
        await message.answer(
            "Доступ запрещен.\n"
            "Для получения доступа обратитесь к администратору.",
            parse_mode="HTML"
        )
        logger.warning(f"Попытка доступа от неавторизованного пользователя: {user_id}")
        return
    
    await state.clear()
    
    welcome_text = (
        "<b>Telegram Analytics Platform</b>\n\n"
        "Выберите режим генерации отчета:"
    )
    
    await message.answer(
        welcome_text,
        reply_markup=create_provider_mode_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(ReportStates.choosing_provider_mode)
    
    logger.info(f"Пользователь {user_id} (@{message.from_user.username}) запустил диалог")


@dp.message(Command("help"))
async def help_handler(message: Message):
    """Справка по работе с ботом"""
    if not is_user_allowed(message.from_user.id):
        return
    
    help_text = (
        "<b>Инструкция по использованию</b>\n\n"
        "<b>Доступные команды:</b>\n"
        "/start - начать создание отчета\n"
        "/status - проверить активные задачи\n"
        "/cancel - отменить текущий диалог\n"
        "/help - эта справка\n\n"
        "<b>Процесс создания отчета:</b>\n"
        "1. Выберите тип отчета\n"
        "2. Укажите период парсинга\n"
        "3. Дождитесь готового файла .docx\n\n"
        "<b>Особенности:</b>\n"
        "• Генерация отчета занимает 2-5 минут\n"
        "• Одновременно можно обрабатывать только один запрос\n"
        "• Доступен парсинг от 1 до 365 дней"
    )
    await message.answer(help_text, parse_mode="HTML")


@dp.message(Command("status"))
async def status_handler(message: Message):
    """Проверка активных задач пользователя"""
    if not is_user_allowed(message.from_user.id):
        return
    
    user_id = message.from_user.id
    task_info = task_manager.get_task_info(user_id)
    
    if task_info and not task_info["done"]:
        await message.answer(
            f"<b>Статус:</b> задача в обработке\n"
            f"<b>User ID:</b> <code>{user_id}</code>\n\n"
            f"Готовый файл будет отправлен автоматически.",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "<b>Статус:</b> нет активных задач\n\n"
            "Используйте /start для создания нового отчета.",
            parse_mode="HTML"
        )


@dp.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext):
    """Отмена текущего диалога"""
    if not is_user_allowed(message.from_user.id):
        return
    
    user_id = message.from_user.id
    current_state = await state.get_state()
    
    # Проверяем через TaskManager
    task_info = task_manager.get_task_info(user_id)
    if task_info and not task_info["done"]:
        await message.answer(
            "Невозможно отменить задачу, которая уже запущена.\n"
            "Дождитесь завершения генерации отчета.",
            parse_mode="HTML"
        )
    elif current_state:
        await state.clear()
        await message.answer(
            "Диалог отменен.\n"
            "Используйте /start для создания нового отчета.",
            parse_mode="HTML"
        )
        logger.info(f"Пользователь {user_id} отменил диалог")
    else:
        await message.answer(
            "Нет активного диалога для отмены.",
            parse_mode="HTML"
        )


# ============================================================================
# ОБРАБОТЧИКИ CALLBACK'ОВ
# ============================================================================

@dp.callback_query(F.data.startswith("mode_"), StateFilter(ReportStates.choosing_provider_mode))
async def provider_mode_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора режима провайдера (Free/Paid)"""
    # Rate limit по пользователю (анти-спам)
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    
    provider_mode = callback.data.replace("mode_", "")
    await state.update_data(provider_mode=provider_mode)
    
    mode_names = {
        "free": "🆓 Бесплатно (Google)",
        "paid": "💳 Платно (Polza.ai)"
    }
    
    text = (
        f"<b>Режим:</b> {mode_names.get(provider_mode, provider_mode)}\n\n"
        f"Выберите подборку Telegram-каналов для анализа:"
    )
    
    await safe_call(
        callback.message.edit_text(
            text,
            reply_markup=create_source_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="edit_text(source_after_mode)"
    )
    await state.set_state(ReportStates.choosing_channel_source)
    await safe_call(callback.answer(), op_desc="callback.answer(mode)")
    
    logger.info(f"Пользователь {callback.from_user.id} выбрал режим провайдера: {provider_mode}")


@dp.callback_query(F.data.startswith("source_"), StateFilter(ReportStates.choosing_channel_source))
async def source_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора источника каналов"""
    # Rate limit по пользователю (анти-спам)
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    source_id = callback.data.replace("source_", "", 1)
    await state.update_data(channel_source=source_id)

    text = (
        "<b>Источник выбран.</b>\n\n"
        "Выберите тип отчета:"
    )

    await safe_call(
        callback.message.edit_text(
            text,
            reply_markup=create_report_type_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="edit_text(report_type_after_source)"
    )
    await state.set_state(ReportStates.choosing_report_type)
    await safe_call(callback.answer(), op_desc="callback.answer(source)")
    
    logger.info(f"Пользователь {callback.from_user.id} выбрал источник: {source_id}")

@dp.callback_query(F.data.startswith("type_"), StateFilter(ReportStates.choosing_report_type))
async def report_type_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора типа отчета"""
    report_type = callback.data.replace("type_", "")
    # Rate limit по пользователю (анти-спам)
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    await state.update_data(report_type=report_type)
    
    type_names = {
        "news": "Дайджест новостей",
        "events": "Календарь мероприятий",
        "custom_task_1": "Доп. сценарий",
        "custom_task_2": "Доп. сценарий 2"
    }
    
    text = (
        f"<b>Выбран тип:</b> {type_names.get(report_type, report_type)}\n\n"
        f"Укажите период парсинга:"
    )
    
    await safe_call(
        callback.message.edit_text(
            text,
            reply_markup=create_period_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="edit_text(period_keyboard)"
    )
    await state.set_state(ReportStates.choosing_period)
    await safe_call(callback.answer(), op_desc="callback.answer(type)")
    
    logger.info(f"Пользователь {callback.from_user.id} выбрал тип: {report_type}")


@dp.callback_query(F.data.startswith("period_"), StateFilter(ReportStates.choosing_period))
async def period_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора периода"""
    period = callback.data.replace("period_", "")
    # Rate limit по пользователю (анти-спам)
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    
    if period in ["today", "yesterday"]:
        await state.update_data(period=period)
        
        logger.info(f"Пользователь {callback.from_user.id} выбрал период: {period}")
        
        # Переходим к выбору модели
        period_names = {"today": "за сегодня", "yesterday": "за вчера"}
        text = (
            f"<b>Период:</b> {period_names.get(period)}\n\n"
            f"Выберите модель:"
        )
        
        await safe_call(
            callback.message.edit_text(
                text,
                reply_markup=create_model_keyboard(),
                parse_mode="HTML"
            ),
            op_desc="edit_text(model_keyboard)"
        )
        await state.set_state(ReportStates.choosing_model)
        await safe_call(callback.answer(), op_desc="callback.answer(period)")
    
    elif period == "manual":
        # Единый текст для всех пользователей с предупреждением о длительности
        text = (
            "<b>Укажите количество дней для парсинга</b>\n\n"
            "⚠️ <b>Внимание:</b> парсинг за период более 3 дней может занять "
            "значительное время и привести к таймауту.\n\n"
            "Примеры:\n"
            "• <code>1</code> - за последний день\n"
            "• <code>3</code> - за последние 3 дня\n"
            "• <code>7</code> - за последнюю неделю\n"
            "• <code>30</code> - за последний месяц\n\n"
            "Диапазон: от 1 до 365 дней\n\n"
            "Отправьте число:"
        )
        await safe_call(
            callback.message.edit_text(
                text,
                reply_markup=create_cancel_keyboard(),
                parse_mode="HTML"
            ),
            op_desc="edit_text(manual_days)"
        )
        # Сохраняем ID сообщения с инструкцией для последующего удаления
        await state.update_data(instruction_message_id=callback.message.message_id)
        await state.set_state(ReportStates.entering_days)
        await safe_call(callback.answer(), op_desc="callback.answer(manual)")


@dp.callback_query(F.data.startswith("model_"), StateFilter(ReportStates.choosing_model))
async def model_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора модели Gemini"""
    # Rate limit по пользователю (анти-спам)
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    
    model_alias = callback.data.replace("model_", "")
    await state.update_data(model_alias=model_alias)
    
    logger.info(f"Пользователь {callback.from_user.id} выбрал модель: {model_alias}")
    logger.info(f"Selected model: {model_alias}")
    
    # КРИТИЧНО: Сначала отвечаем Telegram, потом запускаем долгую операцию
    await safe_call(callback.answer(), op_desc="callback.answer(model)")
    
    # Запускаем генерацию отчета в фоне
    asyncio.create_task(
        start_report_generation(
            user_id=callback.from_user.id,
            chat_id=callback.message.chat.id,
            state=state,
            menu_message_id=callback.message.message_id
        )
    )


@dp.callback_query(F.data == "nav_back_to_period")
async def nav_back_to_period_callback(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору периода"""
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    
    data = await state.get_data()
    report_type = data.get("report_type", "news")
    
    type_names = {
        "news": "Дайджест новостей",
        "events": "Календарь мероприятий",
        "custom_task_1": "Доп. сценарий",
        "custom_task_2": "Доп. сценарий 2"
    }
    
    text = (
        f"<b>Выбран тип:</b> {type_names.get(report_type, report_type)}\n\n"
        f"Укажите период парсинга:"
    )
    
    await safe_call(
        callback.message.edit_text(
            text,
            reply_markup=create_period_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="nav_back_to_period edit_text"
    )
    await state.set_state(ReportStates.choosing_period)
    await safe_call(callback.answer(), op_desc="callback.answer(nav_back_to_period)")


@dp.message(StateFilter(ReportStates.entering_days))
async def days_input_handler(message: Message, state: FSMContext):
    """Обработка ручного ввода количества дней"""
    text = message.text.strip()
    
    # Валидация: проверка на число
    if not text.isdigit():
        await message.answer(
            "Пожалуйста, отправьте число.\n"
            "Например: <code>3</code>",
            parse_mode="HTML"
        )
        return
    
    days = int(text)

    # Валидация: диапазон значений
    if days < 1 or days > 365:
        await message.answer(
            "Количество дней должно быть от <b>1</b> до <b>365</b>.\n"
            "Попробуйте снова:",
            parse_mode="HTML"
        )
        return
    
    # Извлекаем ID сообщения с инструкцией для удаления
    data = await state.get_data()
    instruction_message_id = data.get("instruction_message_id")
    
    # Удаляем сообщение пользователя с числом для чистоты чата
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщение пользователя: {e}")
    
    await state.update_data(days=days)
    
    logger.info(f"Пользователь {message.from_user.id} указал период: {days} дней")
    
    # Переходим к выбору модели
    model_text = (
        f"<b>Период:</b> за последние {days} дн.\n\n"
        f"Выберите модель Gemini для генерации:"
    )
    
    # Редактируем сообщение с инструкцией
    if instruction_message_id:
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=instruction_message_id,
                text=model_text,
                reply_markup=create_model_keyboard(),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение: {e}")
            await message.answer(
                model_text,
                reply_markup=create_model_keyboard(),
                parse_mode="HTML"
            )
    else:
        await message.answer(
            model_text,
            reply_markup=create_model_keyboard(),
            parse_mode="HTML"
        )
    
    await state.set_state(ReportStates.choosing_model)


# ============================================================================
# НАВИГАЦИЯ
# ============================================================================

@dp.callback_query(F.data == "nav_back_to_provider")
async def nav_back_to_provider_callback(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору режима провайдера"""
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    
    text = (
        "<b>Telegram Analytics Platform</b>\n\n"
        "Выберите режим генерации отчета:"
    )
    
    await safe_call(
        callback.message.edit_text(
            text,
            reply_markup=create_provider_mode_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="nav_back_to_provider edit_text"
    )
    await state.set_state(ReportStates.choosing_provider_mode)
    await safe_call(callback.answer(), op_desc="callback.answer(nav_back_to_provider)")


@dp.callback_query(F.data == "nav_back")
async def nav_back_callback(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору типа отчета"""
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    text = "Выберите тип отчета:"
    
    await safe_call(
        callback.message.edit_text(
            text,
            reply_markup=create_report_type_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="nav_back edit_text"
    )
    await state.set_state(ReportStates.choosing_report_type)
    await safe_call(callback.answer(), op_desc="callback.answer(nav_back)")


@dp.callback_query(F.data == "nav_reset")
async def nav_reset_callback(callback: CallbackQuery, state: FSMContext):
    """Сброс диалога и начало заново"""
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    await state.clear()
    
    welcome_text = (
        "<b>Telegram Analytics Platform</b>\n\n"
        "Выберите режим генерации отчета:"
    )
    
    try:
        await callback.message.delete()
    except Exception:
        pass  # Игнорируем ошибки удаления
    
    await safe_call(
        callback.message.answer(
            welcome_text,
            reply_markup=create_provider_mode_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="nav_reset answer(provider_mode_menu)"
    )
    await state.set_state(ReportStates.choosing_provider_mode)
    await safe_call(callback.answer("Диалог сброшен"), op_desc="callback.answer(nav_reset)")


@dp.callback_query(F.data == "nav_start")
async def nav_start_callback(callback: CallbackQuery, state: FSMContext):
    """Начало нового отчета (после завершения предыдущего)"""
    if rate_limited(callback.from_user.id):
        await safe_call(callback.answer(), op_desc="callback.answer(rate_limited)")
        return
    await state.clear()
    
    welcome_text = (
        "<b>Telegram Analytics Platform</b>\n\n"
        "Выберите режим генерации отчета:"
    )
    
    # Убираем кнопки из старого сообщения (не удаляем само сообщение)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass  # Игнорируем ошибки редактирования
    
    # Отправляем новое меню (с выбора провайдера)
    await safe_call(
        callback.message.answer(
            welcome_text,
            reply_markup=create_provider_mode_keyboard(),
            parse_mode="HTML"
        ),
        op_desc="nav_start answer(provider_mode_menu)"
    )
    await state.set_state(ReportStates.choosing_provider_mode)
    await safe_call(callback.answer(), op_desc="callback.answer(nav_start)")


# ============================================================================
# ЛОГИКА ГЕНЕРАЦИИ ОТЧЕТА
# ============================================================================

async def start_report_generation(user_id: int, chat_id: int, state: FSMContext, menu_message_id: int = None):
    """
    Инициирует процесс генерации отчета.
    
    Args:
        user_id: ID пользователя Telegram (для отслеживания задач)
        chat_id: ID чата для отправки сообщений
        state: FSM контекст с данными о выборе пользователя
        menu_message_id: ID сообщения с меню для удаления (опционально)
    
    Этапы:
    1. Проверка на наличие активной задачи
    2. Формирование параметров API
    3. Регистрация задачи
    4. Удаление старого меню
    5. Запуск фоновой обработки
    6. Уведомление пользователя
    """
    
    # Защита от повторных запросов через TaskManager
    if not await task_manager.can_start_task(user_id):
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "<b>Предыдущий запрос в обработке</b>\n\n"
                "Дождитесь завершения текущей задачи.\n"
                "Генерация отчета может занимать несколько минут "
                "в зависимости от объема данных."
            ),
            parse_mode="HTML"
        )
        return
    
    # Извлечение данных из FSM
    data = await state.get_data()
    report_type = data.get("report_type")
    period = data.get("period")
    days = data.get("days")
    model_alias = data.get("model_alias", "flash_2_5")
    provider_mode = data.get("provider_mode", "free")
    
    # Формирование параметров API
    api_params = {"report_type": report_type}
    
    if period:
        api_params["period"] = period
    elif days:
        api_params["days"] = days
    else:
        await bot.send_message(
            chat_id=chat_id,
            text="Ошибка: не указан период парсинга",
            parse_mode="HTML"
        )
        logger.error(f"Пользователь {user_id}: отсутствует период в FSM данных")
        return

    # Передаем источник каналов (если выбран)
    channel_source = data.get("channel_source")
    if channel_source:
        api_params["channel_source"] = channel_source
    
    # Передаем выбранную модель и режим провайдера
    api_params["model_alias"] = model_alias
    api_params["provider_mode"] = provider_mode
    logger.info(f"Selected model: {model_alias}, provider: {provider_mode}")
    
    # Генерация уникального ID задачи для логирования
    task_id = f"{user_id}_{int(datetime.now().timestamp())}"
    
    # Удаление старого меню с кнопками (если передан)
    if menu_message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=menu_message_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить меню: {e}")
    
    # Форматирование описания для пользователя
    type_names = {
        "news": "Дайджест новостей",
        "events": "Календарь мероприятий",
        "custom_task_1": "Доп. сценарий",
        "custom_task_2": "Доп. сценарий 2"
    }
    
    period_names = {
        "today": "за сегодня",
        "yesterday": "за вчера"
    }
    
    model_names = {
        "flash_2_5": "Gemini 2.5 Flash",
        "flash_3_0": "Gemini 3.0 Flash"
    }
    
    provider_names = {
        "free": "🆓 Google Gemini",
        "paid": "💳 Polza.ai"
    }
    
    period_display = period_names.get(period) if period else f"за последние {days} дн."
    model_display = model_names.get(model_alias, model_alias)
    provider_display = provider_names.get(provider_mode, provider_mode)
    
    # Отправка статусного сообщения
    status_message = await bot.send_message(
        chat_id=chat_id,
        text=(
            f"<b>Задача принята в обработку</b>\n\n"
            f"<b>Тип отчета:</b> {type_names.get(report_type, report_type)}\n"
            f"<b>Период:</b> {period_display}\n"
            f"<b>Модель:</b> {model_display}\n"
            f"<b>Провайдер:</b> {provider_display}\n"
            f"<b>ID задачи:</b> <code>{task_id}</code>\n\n"
            f"<b>Статус:</b> парсинг Telegram-каналов...\n"
            f"<b>Ожидаемое время:</b> 2-5 минут\n\n"
            f"Готовый файл будет отправлен автоматически."
        ),
        parse_mode="HTML"
    )
    
    # Запуск фоновой задачи через TaskManager
    task = asyncio.create_task(
        process_report_task(user_id, chat_id, api_params, status_message.message_id, task_id)
    )
    task_manager.add_task(user_id, task)
    
    # Очистка FSM
    await state.clear()
    
    logger.info(
        f"Запущена задача {task_id} для пользователя {user_id} "
        f"с параметрами: {api_params}"
    )


async def process_report_task(user_id: int, chat_id: int, api_params: dict, status_message_id: int, task_id: str):
    """
    Фоновая обработка задачи генерации отчета.
    
    Args:
        user_id: ID пользователя Telegram (для трекинга задач)
        chat_id: ID чата для отправки сообщений
        api_params: Параметры для API запроса
        status_message_id: ID статусного сообщения для обновления/удаления
        task_id: Уникальный ID задачи для логирования
    """
    try:
        logger.info(f"Задача {task_id}: начало обработки")
        
        # Вызов API
        file_content, filename = await call_report_api(api_params)
        
        logger.info(f"Задача {task_id}: файл получен ({len(file_content)} байт)")
        
        # Подготовка файла для отправки (aiogram 3 требует BufferedInputFile)
        document = BufferedInputFile(file_content, filename=filename)
        
        # Отправка файла пользователю
        await bot.send_document(
            chat_id=chat_id,
            document=document,
            caption=(
                f"<b>✅ Отчет готов</b>\n\n"
                f"<b>Файл:</b> <code>{filename}</code>\n"
                f"<b>Размер:</b> {len(file_content) / 1024:.1f} КБ\n"
                f"<b>Тип:</b> {api_params['report_type']}"
            ),
            reply_markup=create_restart_keyboard(),
            parse_mode="HTML"
        )
        
        # Удаление статусного сообщения
        try:
            await bot.delete_message(chat_id=chat_id, message_id=status_message_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить статусное сообщение: {e}")
        
        logger.info(f"Задача {task_id}: успешно завершена")
    
    except Exception as e:
        error_text = str(e)
        logger.error(f"Задача {task_id}: ошибка - {error_text}")
        
        # Удаление статусного сообщения при ошибке
        try:
            await bot.delete_message(chat_id=chat_id, message_id=status_message_id)
        except Exception as del_err:
            logger.warning(f"Не удалось удалить статусное сообщение при ошибке: {del_err}")
        
        # Формируем понятное сообщение об ошибке
        if "не найдено ни одного сообщения" in error_text.lower():
            user_message = (
                "<b>❌ Сообщения не найдены</b>\n\n"
                "За указанный период в отслеживаемых каналах не было публикаций.\n\n"
                "<b>Попробуйте:</b>\n"
                "• Увеличить период парсинга\n"
                "• Выбрать другой тип отчета\n"
                "• Попробовать позже"
            )
        elif "timeout" in error_text.lower() or "таймаут" in error_text.lower():
            user_message = (
                "<b>⏱ Превышено время ожидания</b>\n\n"
                "Обработка заняла слишком много времени.\n\n"
                "<b>Попробуйте:</b>\n"
                "• Уменьшить период парсинга\n"
                "• Повторить попытку через несколько минут"
            )
        else:
            user_message = (
                "<b>❌ Ошибка при генерации отчета</b>\n\n"
                f"<code>{error_text[:200]}</code>\n\n"
                "<b>Рекомендации:</b>\n"
                "• Попробуйте еще раз через несколько минут\n"
                "• Уменьшите период парсинга\n"
                "• Обратитесь к администратору, если проблема повторяется"
            )
        
        # Отправка уведомления об ошибке с кнопкой для нового отчета
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=user_message,
                reply_markup=create_restart_keyboard(),
                parse_mode="HTML"
            )
        except Exception as send_err:
            logger.error(f"Не удалось отправить сообщение об ошибке: {send_err}")
    
    finally:
        # Автоматическая очистка через TaskManager (callback уже установлен)
        # Явное удаление больших объектов из RAM
        if 'file_content' in locals() and file_content:
            file_size_mb = len(file_content) / (1024 * 1024)
            logger.info(f"EXPLICIT CLEANUP (RAM): Clearing file_content ~{file_size_mb:.1f}MB")
            file_content = None
        
        logger.info(f"Задача {task_id}: завершена (user {user_id})")


# ============================================================================
# ЗАПУСК БОТА
# ============================================================================

async def on_startup():
    """Действия при запуске бота"""
    logger.info("=" * 60)
    logger.info("Telegram Report Bot запущен")
    logger.info(f"API endpoint: {API_BASE_URL}")
    logger.info(f"API timeout: {API_TIMEOUT} секунд")
    
    allowed = parse_allowed_users()
    if allowed:
        logger.info(f"Режим доступа: только разрешенные пользователи ({len(allowed)})")
    else:
        logger.info("Режим доступа: открытый (все пользователи)")
    
    logger.info("=" * 60)


async def on_shutdown():
    """Действия при остановке бота"""
    logger.info("Остановка бота...")
    
    # Уведомление пользователей с активными задачами
    active_users = task_manager.get_active_users()
    for user_id in active_users:
        try:
            await bot.send_message(
                chat_id=user_id,
                text="Бот был перезапущен. Ваша задача была прервана."
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
    
    await bot.session.close()
    logger.info("Бот остановлен")


# ============================================================================
# WEBHOOK MODE (интеграция с FastAPI)
# ============================================================================
#
# Этот модуль импортируется в app.py для работы в webhook-режиме.
# 
# Запуск: uvicorn app:app --host 0.0.0.0 --port 80
#
# Бот работает через webhook (Telegram отправляет обновления на /telegram-webhook),
# а не через polling. Это production-ready подход для Amvera Cloud.
#
# Переменные окружения для Amvera:
#   - TELEGRAM_BOT_TOKEN: токен бота от @BotFather
#   - WEBHOOK_HOST: https://parser-username.amvera.io
#   - ALLOWED_USERS: telegram user IDs (опционально, через запятую)
#   - API_BASE_URL: http://localhost:8000 (на Amvera автоматически localhost)
#
# ============================================================================

logger.info("📦 Telegram bot module загружен (webhook mode)")


# Для локальной разработки с polling (только для тестов)
if __name__ == "__main__":
    import asyncio
    
    async def main():
        """Локальный запуск с polling (только для разработки)"""
        logger.warning("⚠️  Запуск в режиме polling (только для локальной разработки)")
        logger.warning("⚠️  На production используйте: uvicorn app:app")
        
        try:
            await on_startup()
            await dp.start_polling(bot)
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки (Ctrl+C)")
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
        finally:
            await on_shutdown()
    
    asyncio.run(main())

