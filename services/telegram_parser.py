"""
Сервис для парсинга Telegram-каналов
"""
import asyncio
import json
import os
import logging
import aiofiles
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from telethon import TelegramClient
from telethon.errors import ChannelInvalidError, ChannelPrivateError, ChannelPublicGroupNaError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message


# Логирование
logger = logging.getLogger(__name__)

# Конфигурация детального логирования парсера (через переменную окружения)
DETAILED_PARSER_LOGGING = os.getenv("DETAILED_PARSER_LOGGING", "False").lower() in ("true", "1", "t")

# Константы
API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
SESSION_NAME = 'telegram_parser_session'
CHANNELS_FILE = 'source_1.json'  # Дефолтный источник каналов
TIMEZONE = 'Europe/Moscow'


async def parse_channel(client: TelegramClient, channel_name: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Парсит сообщения из указанного канала в диапазоне [start_date, end_date).

    :param client: Экземпляр TelegramClient.
    :param channel_name: Имя или ссылка на канал.
    :param start_date: Дата начала (включительно).
    :param end_date: Дата окончания (не включительно).
    :return: Список словарей с данными сообщений.
    """
    try:
        # Присоединяемся к каналу (если еще не подписаны)
        try:
            await client(JoinChannelRequest(channel_name))
        except Exception:
            pass

        collected_messages = []

        # Итерация по сообщениям канала (от новых к старым)
        async for message in client.iter_messages(channel_name):
            # Получаем дату сообщения (timezone-aware)
            message_date = message.date
            
            # Если сообщение старше start_date, прекращаем итерацию
            if message_date < start_date:
                break
            
            # Проверяем, что сообщение в диапазоне [start_date, end_date)
            if message_date >= end_date:
                continue
            
            # Проверяем, что это сообщение с текстом
            if isinstance(message, Message) and message.text:
                collected_messages.append({
                    "channel_name": channel_name,
                    "message_id": message.id,
                    "publication_date": message_date.isoformat(),
                    "text": message.text
                })
        
        if DETAILED_PARSER_LOGGING:
            logger.info(f"Канал '{channel_name}' обработан. Найдено сообщений: {len(collected_messages)}")

        return collected_messages

    except (ChannelInvalidError, ChannelPrivateError, ChannelPublicGroupNaError) as e:
        # Тихо пропускаем недоступные каналы
        return []
    except Exception as e:
        # Тихо пропускаем каналы с ошибками
        return []


async def parse_telegram_channels(start_date: datetime, end_date: datetime, channel_source: str = "source_1") -> list[dict]:
    """
    Парсит все каналы из указанного источника за указанный период.
    
    :param start_date: Дата начала парсинга (включительно).
    :param end_date: Дата окончания парсинга (не включительно).
    :param channel_source: Источник каналов (source_1, source_2, ..., source_6).
    :return: Список всех собранных сообщений.
    """
    # Загрузка списка каналов по источнику
    # Маппинг анонимизированных источников на файлы с каналами
    # Переименуйте ваши файлы channels_*.json в source_N.json
    source_map = {
        "source_1": "source_1.json",
        "source_2": "source_2.json",
        "source_3": "source_3.json",
        "source_4": "source_4.json",
        "source_5": "source_5.json",
        "source_6": "source_6.json",
    }
    file_name = source_map.get(channel_source, "source_1.json")
    channels_path = f"/data/{file_name}" if "AMVERA" in os.environ else file_name
    
    try:
        async with aiofiles.open(channels_path, 'r', encoding='utf-8') as file:
            content = await file.read()
            channels = await asyncio.to_thread(json.loads, content)
    except FileNotFoundError:
        raise FileNotFoundError(f"Файл {channels_path} не найден.")
    except json.JSONDecodeError:
        raise ValueError(f"Не удалось декодировать JSON в файле {channels_path}.")

    # Инициализация клиента Telegram
    session_path = "/data/telegram_parser_session" if "AMVERA" in os.environ else SESSION_NAME
    
    if not API_ID or not API_HASH:
        raise ValueError("Не установлены TELEGRAM_API_ID и TELEGRAM_API_HASH")
    
    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        await client.start()
        
        # Параллельный парсинг всех каналов
        tasks = [parse_channel(client, channel, start_date, end_date) for channel in channels]
        results = await asyncio.gather(*tasks)

        # Итоговый лог по всем каналам (всегда включен)
        all_messages = [message for result in results for message in result]
        logger.info(f"Парсинг завершен. Обработано каналов: {len(channels)}. Всего найдено сообщений: {len(all_messages)}")
        return all_messages


def calculate_date_range(days: int = None, period: str = None) -> tuple[datetime, datetime]:
    """
    Рассчитывает диапазон дат для парсинга.
    
    :param days: Количество дней для архивного режима.
    :param period: 'today' для оперативного режима.
    :return: Кортеж (start_date, end_date).
    """
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)
    
    if period == "today":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now
    elif period == "yesterday":
        # Вчерашний день: с 00:00:00 до 23:59:59 вчера
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = today_start - timedelta(days=1)
        end_date = today_start  # Конец вчерашнего дня = начало сегодняшнего
    elif days:
        end_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=days)
    else:
        raise ValueError("Необходимо указать либо days, либо period='today'/'yesterday'")
    
    return start_date, end_date

