"""
Модуль автоматической очистки устаревших файлов (File Retention Policy).

Ключевые принципы:
- Работает ТОЛЬКО с /data/raw_parses/
- ЗАПРЕЩЕНО трогать /data/temp/
- Запуск ежедневно в 03:00 по Московскому времени
- Удаляет файлы старше RETENTION_DAYS дней
- Использует чистый asyncio без тяжёлых библиотек
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

# Логирование
logger = logging.getLogger(__name__)

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Московский часовой пояс
TIMEZONE = 'Europe/Moscow'

# Количество дней хранения файлов (по умолчанию 7)
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))

# Час запуска очистки по МСК (по умолчанию 3 часа ночи)
CLEANUP_TIME_HOUR = int(os.getenv("CLEANUP_TIME_HOUR", "3"))

# Флаг включения/выключения модуля
ENABLE_FILE_CLEANUP = os.getenv("ENABLE_FILE_CLEANUP", "true").lower() in ("true", "1", "t")

# ============================================================================
# БЕЗОПАСНОСТЬ: Жёсткие ограничения
# ============================================================================

# ТОЛЬКО эта директория подлежит очистке (абсолютный путь для Amvera)
TARGET_DIRECTORY = "/data/raw_parses"

# ЗАПРЕЩЁННЫЕ пути — НИ ПРИ КАКИХ УСЛОВИЯХ не трогаем
FORBIDDEN_PATHS = [
    "/data/temp",
    "/data/telegram_parser_session",  # Сессия Telegram
]


def is_path_safe(filepath: str) -> bool:
    """
    Проверяет, безопасно ли удалять файл.
    
    :param filepath: Полный путь к файлу
    :return: True если файл можно удалить, False если защищён
    """
    # Нормализуем путь
    normalized_path = os.path.normpath(filepath)
    
    # Проверяем, что путь находится ВНУТРИ целевой директории
    if not normalized_path.startswith(TARGET_DIRECTORY):
        logger.warning(f"⚠️ Попытка удалить файл вне целевой директории: {filepath}")
        return False
    
    # Проверяем запрещённые пути
    for forbidden in FORBIDDEN_PATHS:
        if normalized_path.startswith(forbidden):
            logger.warning(f"⚠️ Попытка удалить файл из защищённой директории: {filepath}")
            return False
    
    return True


# ============================================================================
# ВЫЧИСЛЕНИЕ ВРЕМЕНИ
# ============================================================================

def calculate_seconds_until_target_time(target_hour: int) -> float:
    """
    Вычисляет количество секунд до указанного часа по Московскому времени.
    
    Если текущее время уже прошло target_hour сегодня,
    возвращает время до target_hour завтра.
    
    :param target_hour: Целевой час (0-23)
    :return: Количество секунд до целевого времени
    """
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)
    
    # Целевое время сегодня
    target_today = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    
    # Если целевое время уже прошло сегодня — берём завтра
    if now >= target_today:
        target_time = target_today + timedelta(days=1)
    else:
        target_time = target_today
    
    # Разница в секундах
    delta = target_time - now
    return delta.total_seconds()


# ============================================================================
# ОЧИСТКА ФАЙЛОВ
# ============================================================================

async def cleanup_old_files() -> dict:
    """
    Удаляет файлы в TARGET_DIRECTORY, которые старше RETENTION_DAYS дней.
    
    ВАЖНО: Работает ТОЛЬКО с /data/raw_parses/
    НЕ ТРОГАЕТ /data/temp/ и другие защищённые директории.
    
    :return: Словарь со статистикой: deleted_count, freed_bytes, deleted_files
    """
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)
    cutoff_time = now - timedelta(days=RETENTION_DAYS)
    cutoff_timestamp = cutoff_time.timestamp()
    
    deleted_count = 0
    freed_bytes = 0
    deleted_files = []
    errors = []
    
    # Проверяем существование директории
    if not os.path.exists(TARGET_DIRECTORY):
        logger.warning(f"⚠️ Целевая директория не существует: {TARGET_DIRECTORY}")
        return {
            "deleted_count": 0,
            "freed_bytes": 0,
            "deleted_files": [],
            "errors": [f"Directory not found: {TARGET_DIRECTORY}"]
        }
    
    logger.info(f"🔍 Начинаю сканирование {TARGET_DIRECTORY} (порог: {RETENTION_DAYS} дней)")
    
    try:
        # Обходим все файлы в директории (рекурсивно)
        for root, dirs, files in os.walk(TARGET_DIRECTORY):
            for filename in files:
                filepath = os.path.join(root, filename)
                
                try:
                    # Проверка безопасности
                    if not is_path_safe(filepath):
                        continue
                    
                    # Получаем время последней модификации
                    stat_info = os.stat(filepath)
                    file_mtime = stat_info.st_mtime
                    file_size = stat_info.st_size
                    
                    # Проверяем, устарел ли файл
                    if file_mtime < cutoff_timestamp:
                        # Удаляем файл
                        os.remove(filepath)
                        
                        deleted_count += 1
                        freed_bytes += file_size
                        deleted_files.append({
                            "path": filepath,
                            "size_kb": round(file_size / 1024, 2),
                            "modified": datetime.fromtimestamp(file_mtime, tz).isoformat()
                        })
                        
                        logger.info(f"🗑️ Удалён: {filepath} ({round(file_size / 1024, 2)} KB)")
                        
                except OSError as e:
                    error_msg = f"Ошибка при обработке {filepath}: {e}"
                    errors.append(error_msg)
                    logger.error(f"❌ {error_msg}")
                    
    except Exception as e:
        error_msg = f"Критическая ошибка при сканировании: {e}"
        errors.append(error_msg)
        logger.error(f"❌ {error_msg}", exc_info=True)
    
    # Итоговый лог
    freed_mb = round(freed_bytes / 1024 / 1024, 2)
    logger.info(
        f"✅ Очистка завершена: удалено {deleted_count} файлов, "
        f"освобождено {freed_mb} MB"
    )
    
    return {
        "deleted_count": deleted_count,
        "freed_bytes": freed_bytes,
        "freed_mb": freed_mb,
        "deleted_files": deleted_files,
        "errors": errors,
        "retention_days": RETENTION_DAYS,
        "target_directory": TARGET_DIRECTORY
    }


# ============================================================================
# ФОНОВЫЙ ПРОЦЕСС
# ============================================================================

async def file_cleanup_loop():
    """
    Фоновый процесс ежедневной очистки устаревших файлов.
    
    Алгоритм:
    1. При запуске вычисляет время до CLEANUP_TIME_HOUR по МСК
    2. Засыпает до этого времени
    3. Выполняет очистку
    4. Засыпает ровно на 24 часа
    5. Повторяет шаги 3-4
    
    Это гарантирует выполнение задачи ежедневно в одно и то же время.
    """
    logger.info(
        f"📁 File Retention Policy активирован: "
        f"удаление файлов старше {RETENTION_DAYS} дней, "
        f"запуск в {CLEANUP_TIME_HOUR}:00 MSK"
    )
    
    while True:
        try:
            # Вычисляем время до следующего запуска
            seconds_until_cleanup = calculate_seconds_until_target_time(CLEANUP_TIME_HOUR)
            
            # Логируем время до запуска в читаемом формате
            hours = int(seconds_until_cleanup // 3600)
            minutes = int((seconds_until_cleanup % 3600) // 60)
            
            tz = ZoneInfo(TIMEZONE)
            next_run = datetime.now(tz) + timedelta(seconds=seconds_until_cleanup)
            
            logger.info(
                f"⏰ Следующая очистка через {hours}ч {minutes}мин "
                f"(в {next_run.strftime('%Y-%m-%d %H:%M:%S')} MSK)"
            )
            
            # Ожидаем до назначенного времени
            await asyncio.sleep(seconds_until_cleanup)
            
            # Выполняем очистку
            logger.info("🚀 Запуск плановой очистки файлов...")
            result = await cleanup_old_files()
            
            logger.info(
                f"📊 Результат очистки: {result['deleted_count']} файлов удалено, "
                f"{result['freed_mb']} MB освобождено"
            )
            
            # После выполнения засыпаем ровно на 24 часа
            # Это обеспечивает стабильное выполнение каждый день
            await asyncio.sleep(86400)  # 24 часа = 86400 секунд
            
        except asyncio.CancelledError:
            logger.info("🛑 File cleanup loop остановлен (CancelledError)")
            break
        except Exception as e:
            logger.error(f"❌ Ошибка в file cleanup loop: {e}", exc_info=True)
            # При ошибке ждём 1 час и пробуем снова
            await asyncio.sleep(3600)
