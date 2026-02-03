"""
Модуль управления оперативной памятью (RAM) для предотвращения утечек.

Ключевые принципы из examples_app.py:
- TTL-based автоматическая очистка задач по статусу
- Thread-safe операции через asyncio.Lock
- Ленивая очистка при чтении (expired задачи)
- Агрессивная периодическая очистка (каждые 2 минуты)
- Явное удаление ссылок перед gc.collect()
- Многопоколенная сборка мусора (0→1→2)
- malloc_trim для возврата памяти ОС (Linux)

⚠️ ВАЖНО: Очищает ТОЛЬКО RAM, НЕ трогает файлы на диске!
"""

import gc
import time
import asyncio
import logging
import os
import ctypes
from typing import Dict, Optional, Any
from datetime import datetime

# Импортируем psutil только если доступен
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory metrics will be disabled")


# ============================================================================
# MALLOC_TRIM: Принудительный возврат памяти операционной системе
# ============================================================================

_libc = None
_malloc_trim_available = False

def _init_malloc_trim():
    """
    Инициализация malloc_trim (только Linux).
    Python/glibc удерживает освобожденную память в своих аренах.
    malloc_trim(0) принудительно возвращает её ОС.
    """
    global _libc, _malloc_trim_available
    try:
        _libc = ctypes.CDLL("libc.so.6")
        # Проверяем, что функция доступна
        _libc.malloc_trim.argtypes = [ctypes.c_size_t]
        _libc.malloc_trim.restype = ctypes.c_int
        _malloc_trim_available = True
        logging.getLogger(__name__).info("malloc_trim initialized successfully (Linux)")
    except (OSError, AttributeError) as e:
        # Windows или нестандартный Linux - безопасно игнорируем
        _malloc_trim_available = False
        logging.getLogger(__name__).debug(f"malloc_trim not available: {e}")


def release_memory_to_os() -> bool:
    """
    Принудительно возвращает память операционной системе через malloc_trim.
    
    Безопасен для вызова на любой ОС - на Windows просто ничего не делает.
    
    :return: True если malloc_trim был вызван, False если недоступен
    """
    global _libc, _malloc_trim_available
    
    if not _malloc_trim_available:
        return False
    
    try:
        result = _libc.malloc_trim(0)
        return result == 1  # malloc_trim возвращает 1 при успехе
    except Exception as e:
        logging.getLogger(__name__).warning(f"malloc_trim failed: {e}")
        return False


# Инициализируем при импорте модуля
_init_malloc_trim()


# ============================================================================
# GC TUNING: Более агрессивные пороги сборки мусора
# ============================================================================

def configure_gc():
    """
    Настраивает пороги сборщика мусора для более частой очистки.
    
    По умолчанию Python использует (700, 10, 10):
    - gen0: каждые 700 аллокаций
    - gen1: каждые 10 сборок gen0
    - gen2: каждые 10 сборок gen1
    
    Мы используем (100, 5, 5) для более частой очистки молодых объектов,
    что уменьшает пиковое потребление памяти.
    """
    old_threshold = gc.get_threshold()
    gc.set_threshold(100, 5, 5)
    new_threshold = gc.get_threshold()
    logging.getLogger(__name__).info(
        f"GC thresholds changed: {old_threshold} -> {new_threshold}"
    )


# Настраиваем GC при импорте модуля
configure_gc()


# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

logger = logging.getLogger(__name__)

# Глобальные структуры В ОПЕРАТИВНОЙ ПАМЯТИ
task_memory: Dict[str, Any] = {}  # {task_id: data} - только RAM!
memory_ttl: Dict[str, float] = {}  # {task_id: expiry_timestamp}
memory_lock = asyncio.Lock()

# Process для мониторинга памяти
process = psutil.Process() if PSUTIL_AVAILABLE else None

# TTL правила по типу задачи (в секундах)
TASK_TTL_RULES = {
    "completed": 300,      # 5 минут для завершенных
    "processing": 0,       # Никогда не удалять активные (0 = infinity)
    "error": 300,          # 5 минут для ошибок
    "pending": 0           # Защита для ожидающих задач
}

# Интервал фоновой очистки (по умолчанию 2 минуты)
CLEANUP_INTERVAL = int(os.getenv("MEMORY_CLEANUP_INTERVAL", "120"))

# Включить/выключить фоновую очистку
ENABLE_MEMORY_CLEANUP = os.getenv("ENABLE_MEMORY_CLEANUP", "true").lower() == "true"


# ============================================================================
# CORE FUNCTIONS: Thread-safe операции с памятью
# ============================================================================

async def safe_set_task(task_id: str, data: Any, status: str = "processing"):
    """
    Thread-safe запись задачи в память с автоматическим TTL.
    
    :param task_id: Уникальный идентификатор задачи
    :param data: Данные задачи (dict или None для удаления)
    :param status: Статус задачи для определения TTL
    """
    async with memory_lock:
        if data is None:
            # Удаление задачи
            task_memory.pop(task_id, None)
            memory_ttl.pop(task_id, None)
            logger.debug(f"Task {task_id} removed from memory")
            return
        
        # Сохранение задачи
        task_memory[task_id] = data
        
        # Определяем TTL по статусу
        ttl_seconds = TASK_TTL_RULES.get(status, 3600)  # По умолчанию 1 час
        
        if ttl_seconds > 0:
            expiry_time = time.time() + ttl_seconds
            memory_ttl[task_id] = expiry_time
            logger.debug(f"Task {task_id} saved with TTL={ttl_seconds}s (status={status})")
        else:
            # TTL=0 означает "никогда не удалять"
            memory_ttl.pop(task_id, None)  # Убираем из TTL мониторинга
            logger.debug(f"Task {task_id} saved without TTL (protected, status={status})")


async def safe_get_task(task_id: str) -> Optional[Any]:
    """
    Thread-safe чтение задачи с ленивой очисткой expired.
    
    :param task_id: Уникальный идентификатор задачи
    :return: Данные задачи или None если не найдена/expired
    """
    async with memory_lock:
        # Проверяем TTL
        if task_id in memory_ttl:
            if time.time() > memory_ttl[task_id]:
                # Задача expired - ленивая очистка
                task = task_memory.get(task_id)
                if task:
                    status = task.get("status", "unknown") if isinstance(task, dict) else "unknown"
                    if should_cleanup_by_status(status):
                        logger.debug(f"Task {task_id} expired (TTL), lazy cleanup")
                        task_memory.pop(task_id, None)
                        memory_ttl.pop(task_id, None)
                        return None
        
        return task_memory.get(task_id)


async def safe_remove_task(task_id: str):
    """
    Thread-safe явное удаление задачи из памяти.
    
    :param task_id: Уникальный идентификатор задачи
    """
    await safe_set_task(task_id, None)


def should_cleanup_by_status(status: str) -> bool:
    """
    Определяет, можно ли удалить задачу по статусу.
    Защита активных задач от преждевременного удаления.
    
    :param status: Статус задачи
    :return: True если можно удалить, False если защищена
    """
    # Защищенные статусы (активные задачи)
    protected_statuses = ["processing", "pending"]
    
    if status in protected_statuses:
        return False
    
    # Можно удалять завершенные и ошибочные задачи
    return status in ["completed", "error"]


# ============================================================================
# CLEANUP: Агрессивная очистка памяти
# ============================================================================

async def cleanup_expired_tasks() -> int:
    """
    Агрессивная очистка ТОЛЬКО оперативной памяти.
    НЕ ТРОГАЕТ файлы в data/raw_parses и data/temp!
    
    Процесс:
    1. Находим expired задачи в RAM по TTL
    2. Явно удаляем ссылки на большие объекты
    3. Многопоколенная сборка мусора (gc.collect(0/1/2))
    4. Измеряем освобожденную память через psutil
    
    :return: Количество очищенных задач
    """
    # Измеряем память ДО очистки
    memory_before_mb = 0
    if process:
        memory_before_mb = process.memory_info().rss / (1024 * 1024)
    
    async with memory_lock:
        expired_tasks = []
        current_time = time.time()
        
        # 1. Находим задачи с истекшим TTL
        for task_id, expiry_time in list(memory_ttl.items()):
            if current_time > expiry_time:
                task = task_memory.get(task_id)
                if task:
                    status = task.get("status", "unknown") if isinstance(task, dict) else "unknown"
                    if should_cleanup_by_status(status):
                        expired_tasks.append(task_id)
        
        # 2. Явно удаляем ссылки на большие объекты ПЕРЕД удалением из словарей
        cleaned_memory_estimate = 0
        for task_id in expired_tasks:
            task = task_memory.get(task_id)
            
            if task and isinstance(task, dict):
                # Если есть большие поля, оцениваем размер
                if "file_content" in task and task["file_content"]:
                    size_mb = len(str(task["file_content"])) / (1024 * 1024)
                    cleaned_memory_estimate += size_mb
                    task["file_content"] = None
                
                # Очищаем другие большие поля
                for key in ["telegram_text", "messages", "report_data"]:
                    if key in task and task[key]:
                        task[key] = None
            
            # 3. Удаляем из словарей
            task_memory.pop(task_id, None)
            memory_ttl.pop(task_id, None)
    
    # 4. Многопоколенная сборка мусора (вне lock для производительности)
    # КРИТИЧНО: Делаем GC ВСЕГДА, даже если нет expired задач!
    if expired_tasks:
        # Сборка по поколениям
        collected_0 = gc.collect(0)  # Молодые объекты
        collected_1 = gc.collect(1)  # Средние объекты
        collected_2 = gc.collect(2)  # Старые объекты
        total_collected = collected_0 + collected_1 + collected_2
        
        # Финальная сборка для циклических ссылок
        final_collected = gc.collect()
        total_collected += final_collected
        
        # MEMORY OPTIMIZATION: Принудительно возвращаем память ОС
        malloc_trimmed = release_memory_to_os()
        
        # 5. Измеряем память ПОСЛЕ очистки
        memory_after_mb = 0
        freed_mb = 0
        if process:
            memory_after_mb = process.memory_info().rss / (1024 * 1024)
            freed_mb = memory_before_mb - memory_after_mb
        
        logger.info(
            f"Memory cleanup (RAM only): removed {len(expired_tasks)} tasks, "
            f"collected {total_collected} objects, malloc_trim={malloc_trimmed}, "
            f"freed ~{freed_mb:.1f}MB (RSS: {memory_after_mb:.1f}MB)"
        )
        logger.info(f"Files in data/raw_parses and data/temp are SAFE")
    else:
        # НЕТ expired задач, но делаем ФОРСИРОВАННЫЙ GC
        collected_0 = gc.collect(0)  # Молодые объекты
        
        # MEMORY OPTIMIZATION: Принудительно возвращаем память ОС
        malloc_trimmed = release_memory_to_os()
        
        # Измеряем память ПОСЛЕ форсированного GC
        memory_after_mb = 0
        freed_mb = 0
        if process:
            memory_after_mb = process.memory_info().rss / (1024 * 1024)
            freed_mb = memory_before_mb - memory_after_mb
        
        logger.info(
            f"Memory cleanup (FORCED GC, no expired): "
            f"collected {collected_0} objects, malloc_trim={malloc_trimmed}, "
            f"freed ~{freed_mb:.1f}MB (RSS: {memory_after_mb:.1f}MB)"
        )
    
    return len(expired_tasks)


async def memory_cleanup_loop():
    """
    Фоновый процесс агрессивной очистки RAM.
    Запускается каждые CLEANUP_INTERVAL секунд (по умолчанию 2 минуты).
    
    Каждые 5 циклов делает DEEP GC (gc.collect(2)) для освобождения старых объектов.
    
    ⚠️ ВАЖНО: Очищает ТОЛЬКО оперативную память, файлы на диске НЕ ТРОГАЕТ!
    """
    logger.info(f"Memory cleanup loop started (interval={CLEANUP_INTERVAL}s, RAM only)")
    
    cycle_count = 0
    
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        cycle_count += 1
        
        try:
            # Обычная очистка каждый цикл
            cleaned_count = await cleanup_expired_tasks()
            
            # DEEP GC каждые 5 циклов (каждые 10 минут при интервале 2 мин)
            if cycle_count % 5 == 0:
                memory_before_mb = 0
                if process:
                    memory_before_mb = process.memory_info().rss / (1024 * 1024)
                
                # Глубокая сборка старых объектов
                collected = gc.collect(2)
                
                # MEMORY OPTIMIZATION: Принудительно возвращаем память ОС после DEEP GC
                malloc_trimmed = release_memory_to_os()
                
                memory_after_mb = 0
                freed_mb = 0
                if process:
                    memory_after_mb = process.memory_info().rss / (1024 * 1024)
                    freed_mb = memory_before_mb - memory_after_mb
                
                logger.info(
                    f"DEEP GC (cycle {cycle_count}): "
                    f"collected {collected} objects, malloc_trim={malloc_trimmed}, "
                    f"freed ~{freed_mb:.1f}MB (RSS: {memory_after_mb:.1f}MB)"
                )
            
            if cleaned_count > 0:
                logger.info(f"Aggressive memory cleanup: removed {cleaned_count} expired tasks")
                
        except Exception as e:
            logger.error(f"Error in memory_cleanup_loop: {str(e)}", exc_info=True)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

async def get_memory_stats() -> dict:
    """
    Получить текущую статистику использования памяти.
    
    :return: Словарь с метриками памяти
    """
    async with memory_lock:
        stats = {
            "active_tasks_in_ram": len(task_memory),
            "tasks_with_ttl": len(memory_ttl),
            "gc_counts": gc.get_count(),
            "note": "This shows RAM usage only. Files in /data/ are not affected."
        }
        
        if process:
            mem_info = process.memory_info()
            stats.update({
                "memory_rss_mb": mem_info.rss / (1024 * 1024),
                "memory_vms_mb": mem_info.vms / (1024 * 1024),
            })
        else:
            stats.update({
                "memory_rss_mb": None,
                "memory_vms_mb": None,
                "warning": "psutil not available"
            })
        
        return stats


async def manual_cleanup():
    """
    Ручной запуск очистки памяти (для тестирования).
    
    :return: Количество очищенных задач
    """
    logger.info("Manual memory cleanup triggered")
    return await cleanup_expired_tasks()


async def force_gc() -> dict:
    """
    Принудительная сборка мусора с измерением освобожденной памяти.
    Включает malloc_trim для возврата памяти ОС.
    
    :return: Словарь с результатами GC
    """
    logger.info("Force GC triggered manually")
    
    memory_before_mb = 0
    if process:
        memory_before_mb = process.memory_info().rss / (1024 * 1024)
    
    # Многопоколенная сборка
    collected_0 = gc.collect(0)
    collected_1 = gc.collect(1)
    collected_2 = gc.collect(2)
    total_collected = collected_0 + collected_1 + collected_2
    
    # MEMORY OPTIMIZATION: Принудительно возвращаем память ОС
    malloc_trimmed = release_memory_to_os()
    
    memory_after_mb = 0
    freed_mb = 0
    if process:
        memory_after_mb = process.memory_info().rss / (1024 * 1024)
        freed_mb = memory_before_mb - memory_after_mb
    
    result = {
        "collected_objects": total_collected,
        "malloc_trim_called": malloc_trimmed,
        "freed_mb": round(freed_mb, 2),
        "memory_before_mb": round(memory_before_mb, 2),
        "memory_after_mb": round(memory_after_mb, 2),
        "gc_counts": gc.get_count(),
        "gc_thresholds": gc.get_threshold()
    }
    
    logger.info(
        f"Force GC completed: collected {total_collected} objects, "
        f"malloc_trim={malloc_trimmed}, freed ~{freed_mb:.1f}MB (RSS: {memory_after_mb:.1f}MB)"
    )
    
    return result

