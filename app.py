"""
Главная точка входа FastAPI приложения

Монолитный сервис с модульной архитектурой:
- Парсинг Telegram-каналов (routers/parser.py)
- Генерация отчетов через Gemini (routers/reports.py)
- Telegram Bot через webhook (telegram_bot.py)
"""
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from routers import parser, reports
import os
import gc
import asyncio
import logging

logger = logging.getLogger(__name__)

# Токен администратора для защиты /admin/ эндпоинтов
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Создание приложения
app = FastAPI(
    title="Telegram Analytics Platform",
    description="Единая платформа для парсинга Telegram и генерации аналитических отчетов",
    version="2.0.0"
)

# Подключение роутеров
app.include_router(parser.router)
app.include_router(reports.router)


@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    """
    Middleware для проверки токена администратора на эндпоинтах /admin/*.
    
    Если переменная окружения ADMIN_TOKEN установлена, то все запросы к /admin/*
    должны содержать заголовок X-Admin-Token с правильным значением.
    При несовпадении или отсутствии заголовка — возвращает 401 Unauthorized.
    """
    if request.url.path.startswith("/admin/"):
        # Если токен не настроен — пропускаем проверку (для локальной разработки)
        if ADMIN_TOKEN:
            token = request.headers.get("X-Admin-Token", "")
            if token != ADMIN_TOKEN:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Unauthorized: неверный или отсутствующий X-Admin-Token"}
                )
    return await call_next(request)

@app.get("/")
async def root():
    """Приветственная страница с информацией о доступных endpoint'ах."""
    return JSONResponse(content={
        "message": "Telegram Analytics Platform",
        "version": "2.0.0",
        "services": {
            "parser": {
                "description": "Парсинг Telegram-каналов",
                "endpoints": [
                    "POST /api/parser/parse - Парсинг и возврат данных",
                    "POST /api/parser/parse-and-save - Парсинг и сохранение в файл"
                ]
            },
            "reports": {
                "description": "Генерация аналитических отчетов",
                "endpoints": [
                    "POST /api/reports/generate - Генерация отчета из данных",
                    "POST /api/reports/generate-from-file - Генерация отчета из файла",
                    "GET /api/reports/types - Список типов отчетов"
                ]
            },
            "telegram_bot": {
                "description": "Telegram Bot UI (webhook mode)",
                "webhook": "/telegram-webhook"
            }
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        }
    })


@app.get("/health")
async def health_check():
    """Проверка работоспособности приложения."""
    return JSONResponse(content={
        "status": "OK",
        "service": "Telegram Analytics Platform",
        "modules": ["parser", "reports", "telegram_bot"]
    })


# ============================================================================
# TELEGRAM BOT WEBHOOK INTEGRATION
# ============================================================================

# Глобальные переменные для бота и диспетчера
bot = None
dp = None


@app.on_event("startup")
async def setup_telegram_webhook():
    """
    При старте FastAPI инициализирует Telegram бота и устанавливает webhook.
    Telegram будет отправлять все обновления на /telegram-webhook endpoint.
    """
    global bot, dp
    
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN не установлен - бот не будет запущен")
        return
    
    try:
        # Импортируем модуль бота
        from telegram_bot import bot as telegram_bot_instance, dp as telegram_dp
        bot = telegram_bot_instance
        dp = telegram_dp
        
        # Определяем webhook URL
        # На Amvera: https://parser-username.amvera.io/telegram-webhook
        WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
        
        if not WEBHOOK_HOST:
            logger.warning(
                "WEBHOOK_HOST не установлен - бот не будет работать. "
                "Установите переменную окружения WEBHOOK_HOST (например: https://parser-username.amvera.io)"
            )
            return
        
        WEBHOOK_PATH = "/telegram-webhook"
        WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
        
        # Устанавливаем webhook
        await bot.set_webhook(
            url=WEBHOOK_URL,
            drop_pending_updates=True  # Игнорируем старые обновления
        )
        
        logger.info(f"✅ Telegram Bot webhook установлен: {WEBHOOK_URL}")
        
        # Проверяем статус webhook
        webhook_info = await bot.get_webhook_info()
        logger.info(f"📡 Webhook статус: {webhook_info.url}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации Telegram бота: {e}", exc_info=True)
    
    # Запуск фонового процесса очистки RAM (файлы на диске НЕ ТРОГАЕМ!)
    try:
        from services.memory_manager import memory_cleanup_loop, ENABLE_MEMORY_CLEANUP
        if ENABLE_MEMORY_CLEANUP:
            asyncio.create_task(memory_cleanup_loop())
            logger.info("🧹 Memory cleanup loop started (every 2 minutes, RAM only)")
        else:
            logger.warning("⚠️ Memory cleanup disabled - это может привести к утечке памяти!")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске memory cleanup: {e}", exc_info=True)
    
    # Запуск фонового процесса очистки устаревших файлов в /data/raw_parses/
    try:
        from services.file_cleaner import file_cleanup_loop, ENABLE_FILE_CLEANUP
        if ENABLE_FILE_CLEANUP:
            asyncio.create_task(file_cleanup_loop())
            logger.info("📁 File cleanup loop started (daily at 03:00 MSK, /data/raw_parses/ only)")
        else:
            logger.warning("⚠️ File cleanup disabled via ENABLE_FILE_CLEANUP=false")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске file cleanup: {e}", exc_info=True)


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    """
    Endpoint для приема обновлений от Telegram.
    
    Telegram отправляет сюда все обновления (сообщения, callback'и и т.д.)
    в формате JSON, которые затем обрабатываются диспетчером aiogram.
    
    ВАЖНО: Обрабатываем update в background task, чтобы не блокировать webhook.
    """
    global bot, dp
    
    if not bot or not dp:
        logger.error("Бот не инициализирован")
        # Telegram требует 200 OK даже при ошибках
        return JSONResponse(
            status_code=200,
            content={"ok": False, "error": "Bot not initialized"}
        )
    
    try:
        # Получаем JSON от Telegram
        update_data = await request.json()
        
        # Импортируем Update из aiogram
        from aiogram.types import Update
        
        # Преобразуем в объект Update
        update = Update(**update_data)
        
        # КРИТИЧНО: Обрабатываем update в background task
        # Это позволяет сразу ответить Telegram'у и не блокировать webhook
        asyncio.create_task(dp.feed_update(bot=bot, update=update))
        
        # Сразу отвечаем Telegram'у
        return JSONResponse(content={"ok": True})
    
    except Exception as e:
        logger.error(f"Ошибка обработки webhook: {e}", exc_info=True)
        # Telegram требует 200 OK даже при ошибках
        return JSONResponse(
            status_code=200,
            content={"ok": True}
        )


@app.on_event("shutdown")
async def shutdown_telegram_bot():
    """
    При остановке приложения удаляем webhook и закрываем сессию бота.
    Также выполняем финальную очистку RAM.
    """
    global bot
    
    if bot:
        try:
            await bot.delete_webhook()
            await bot.session.close()
            logger.info("🛑 Telegram Bot webhook удален")
        except Exception as e:
            logger.error(f"Ошибка при остановке бота: {e}")
    
    # Финальная очистка RAM перед остановкой
    try:
        from services.memory_manager import cleanup_expired_tasks
        cleaned = await cleanup_expired_tasks()
        logger.info(f"🧹 Final memory cleanup (RAM): {cleaned} tasks removed")
    except Exception as e:
        logger.error(f"Ошибка при финальной очистке памяти: {e}")


# ============================================================================
# ADMIN ENDPOINTS: Мониторинг памяти
# ============================================================================

@app.get("/admin/memory-stats")
async def memory_stats():
    """
    Текущее состояние оперативной памяти (RAM).
    
    ⚠️ ВАЖНО: Показывает только использование RAM.
    Файлы в /data/ (raw_parses, temp) не учитываются.
    """
    try:
        from services.memory_manager import get_memory_stats
        stats = await get_memory_stats()
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error(f"Ошибка получения статистики памяти: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@app.post("/admin/memory-cleanup")
async def manual_memory_cleanup():
    """
    Ручной запуск очистки RAM (для тестирования).
    
    ⚠️ ВАЖНО: Очищает только RAM, файлы на диске остаются нетронутыми.
    """
    try:
        from services.memory_manager import manual_cleanup
        cleaned_count = await manual_cleanup()
        return JSONResponse(content={
            "success": True,
            "tasks_cleaned": cleaned_count,
            "message": f"Очищено {cleaned_count} задач из RAM. Файлы на диске не тронуты."
        })
    except Exception as e:
        logger.error(f"Ошибка ручной очистки памяти: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@app.post("/admin/force-gc")
async def force_garbage_collection():
    """
    Принудительная сборка мусора с измерением результата.
    
    Выполняет многопоколенную GC и возвращает:
    - Количество собранных объектов
    - Освобожденную память (MB)
    - Текущее состояние памяти
    
    ⚠️ ВАЖНО: Очищает только RAM, файлы на диске не трогаются.
    """
    try:
        from services.memory_manager import force_gc
        result = await force_gc()
        return JSONResponse(content={
            "success": True,
            **result,
            "note": "Files in /data/ are not affected"
        })
    except Exception as e:
        logger.error(f"Ошибка force GC: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@app.get("/admin/memory-detailed")
async def memory_detailed():
    """
    Детальная диагностика памяти: RSS, Page Cache, Buffers.
    
    Показывает разницу между тем, что видит Python (RSS) и 
    тем, что видит Kubernetes (RSS + Page Cache + Buffers).
    
    Page Cache - это файловый кэш Linux, который Kubernetes
    учитывает в метриках memory_working_set.
    """
    import psutil
    
    result = {
        "process": {},
        "system": {},
        "explanation": {}
    }
    
    # Память процесса Python
    try:
        process = psutil.Process()
        mem_info = process.memory_info()
        result["process"] = {
            "rss_mb": round(mem_info.rss / 1024 / 1024, 2),
            "vms_mb": round(mem_info.vms / 1024 / 1024, 2),
            "note": "RSS - реальная память Python процесса"
        }
    except Exception as e:
        result["process"] = {"error": str(e)}
    
    # Системная память из /proc/meminfo (только Linux)
    try:
        with open('/proc/meminfo', 'r') as f:
            meminfo_raw = f.read()
        
        # Парсим /proc/meminfo
        meminfo = {}
        for line in meminfo_raw.strip().split('\n'):
            parts = line.split(':')
            if len(parts) == 2:
                key = parts[0].strip()
                # Значение в kB, конвертируем в MB
                value_str = parts[1].strip().replace(' kB', '').replace(' KB', '')
                try:
                    value_kb = int(value_str)
                    meminfo[key] = round(value_kb / 1024, 2)  # MB
                except ValueError:
                    meminfo[key] = parts[1].strip()
        
        result["system"] = {
            "MemTotal_mb": meminfo.get('MemTotal', 'N/A'),
            "MemFree_mb": meminfo.get('MemFree', 'N/A'),
            "MemAvailable_mb": meminfo.get('MemAvailable', 'N/A'),
            "Cached_mb": meminfo.get('Cached', 'N/A'),
            "Buffers_mb": meminfo.get('Buffers', 'N/A'),
            "Active_mb": meminfo.get('Active', 'N/A'),
            "Inactive_mb": meminfo.get('Inactive', 'N/A'),
            "Slab_mb": meminfo.get('Slab', 'N/A'),
        }
        
        # Расчет того, что видит Kubernetes
        cached = meminfo.get('Cached', 0) if isinstance(meminfo.get('Cached'), (int, float)) else 0
        buffers = meminfo.get('Buffers', 0) if isinstance(meminfo.get('Buffers'), (int, float)) else 0
        rss = result["process"].get("rss_mb", 0)
        
        result["kubernetes_estimate"] = {
            "working_set_estimate_mb": round(rss + cached + buffers, 2),
            "breakdown": {
                "process_rss_mb": rss,
                "page_cache_mb": cached,
                "buffers_mb": buffers
            },
            "note": "K8s memory = RSS + Page Cache + Buffers (примерно)"
        }
        
    except FileNotFoundError:
        result["system"] = {"error": "/proc/meminfo not found (not Linux?)"}
    except Exception as e:
        result["system"] = {"error": str(e)}
    
    result["explanation"] = {
        "RSS": "Resident Set Size - память, занятая Python процессом",
        "Page_Cache": "Файловый кэш Linux - кэшированные JSON, docx файлы",
        "Buffers": "Буферы ядра для блочных устройств",
        "Why_difference": "Amvera показывает RSS + Page Cache + Buffers, а psutil только RSS"
    }
    
    return JSONResponse(content=result)


@app.get("/admin/disk-usage")
async def disk_usage():
    """
    Показывает размер файлов на диске в /data/.
    Помогает понять, сколько данных кэшируется в Page Cache.
    """
    import os
    
    result = {
        "raw_parses": {},
        "temp": {},
        "total": {}
    }
    
    def get_dir_stats(dir_path: str) -> dict:
        """Получает статистику по директории."""
        if not os.path.exists(dir_path):
            return {"error": f"Directory not found: {dir_path}"}
        
        total_size = 0
        file_count = 0
        files_list = []
        
        try:
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    filepath = os.path.join(root, file)
                    try:
                        size = os.path.getsize(filepath)
                        total_size += size
                        file_count += 1
                        # Сохраняем топ-10 самых больших файлов
                        files_list.append({
                            "name": os.path.relpath(filepath, dir_path),
                            "size_kb": round(size / 1024, 2)
                        })
                    except (OSError, IOError):
                        pass
            
            # Сортируем по размеру и берем топ-10
            files_list.sort(key=lambda x: x["size_kb"], reverse=True)
            top_files = files_list[:10]
            
            return {
                "total_size_mb": round(total_size / 1024 / 1024, 2),
                "total_size_kb": round(total_size / 1024, 2),
                "file_count": file_count,
                "top_10_largest": top_files
            }
        except Exception as e:
            return {"error": str(e)}
    
    # Статистика по директориям (абсолютные пути для Amvera)
    result["raw_parses"] = get_dir_stats("/data/raw_parses")
    result["temp"] = get_dir_stats("/data/temp")
    
    # Общий итог
    raw_size = result["raw_parses"].get("total_size_mb", 0)
    temp_size = result["temp"].get("total_size_mb", 0)
    if isinstance(raw_size, (int, float)) and isinstance(temp_size, (int, float)):
        result["total"]["size_mb"] = round(raw_size + temp_size, 2)
    
    result["note"] = "Эти файлы кэшируются Linux в Page Cache и учитываются в метриках Amvera"
    
    return JSONResponse(content=result)

