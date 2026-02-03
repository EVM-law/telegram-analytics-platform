"""
API endpoints для генерации отчетов
"""
import gc
import os
import logging
import asyncio
import aiofiles
from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import Response, JSONResponse
from services.report_generator import (
    generate_report_data,
    generate_docx,
    generate_filename,
    REPORT_TYPES,
    AVAILABLE_MODELS
)
from services.llm_providers import MODEL_MAPPING, DEFAULT_MODEL_ALIAS


# ============================================================================
# УТИЛИТА ДЛЯ ОСВОБОЖДЕНИЯ PAGE CACHE
# ============================================================================

def release_page_cache(file_path: str) -> None:
    """
    Освобождает Page Cache для файла после его чтения.
    Файл остается на диске, но выгружается из RAM ядра Linux.
    
    Работает только на Linux. На Windows безопасно игнорируется.
    """
    try:
        # POSIX_FADV_DONTNEED = 4 (советуем ядру: эти данные больше не нужны в кэше)
        POSIX_FADV_DONTNEED = 4
        fd = os.open(file_path, os.O_RDONLY)
        try:
            file_size = os.fstat(fd).st_size
            os.posix_fadvise(fd, 0, file_size, POSIX_FADV_DONTNEED)
            logging.getLogger(__name__).debug(f"Page cache released for: {file_path}")
        finally:
            os.close(fd)
    except (AttributeError, OSError) as e:
        # AttributeError: Windows не имеет posix_fadvise
        # OSError: файл недоступен или другие ошибки FS
        pass  # Безопасно игнорируем - это оптимизация, не критичная функция

# Доступные режимы провайдеров
AVAILABLE_PROVIDER_MODES = {"free", "paid"}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Отключаем избыточные логи от сторонних библиотек
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("google_genai").setLevel(logging.WARNING)


router = APIRouter(prefix="/api/reports", tags=["reports"])

# Глобальный lock для последовательного парсинга Telegram (Telethon ограничение)
PARSER_LOCK = asyncio.Lock()


@router.post("/generate")
async def generate_report(data: dict = Body(...)):
    """
    Генерирует отчет на основе данных из Telegram.
    
    Ожидает JSON:
    {
        "messages": [...],  // Массив сообщений с полем "text"
        "report_type": "news"  // или "events", "custom_task_1", "custom_task_2"
    }
    """
    messages = data.get("messages")
    report_type = data.get("report_type")
    
    if not messages:
        raise HTTPException(status_code=400, detail="Не указан параметр 'messages'")
    
    if not report_type:
        raise HTTPException(status_code=400, detail="Не указан параметр 'report_type'")
    
    if report_type not in REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Неверный тип отчета. Доступны: {', '.join(REPORT_TYPES.keys())}"
        )
    
    try:
        # Генерация данных через Gemini
        report_data = await generate_report_data(messages, report_type)
        
        # Генерация документа (CPU/IO bound) в отдельном потоке
        doc_buffer = await asyncio.to_thread(generate_docx, report_data, report_type)
        
        # CRITICAL: Извлекаем байты и закрываем буфер СРАЗУ
        file_content = doc_buffer.getvalue()
        doc_buffer.close()
        del doc_buffer
        gc.collect(0)
        
        # Возврат документа
        filename = generate_filename(report_type)
        
        return Response(
            content=file_content,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка генерации отчета: {str(e)}")


@router.post("/generate-from-file")
async def generate_report_from_file(data: dict = Body(...)):
    """
    Генерирует отчет, читая данные из JSON-файла.
    
    Ожидает JSON:
    {
        "source_file": "/data/raw_parses/2025-10-07.json",
        "report_type": "news"
    }
    
    Примечание: На Amvera используйте абсолютный путь /data/...
    """
    import json
    import os
    from pathlib import Path
    
    source_file = data.get("source_file")
    report_type = data.get("report_type")
    
    if not source_file:
        raise HTTPException(status_code=400, detail="Не указан параметр 'source_file'")
    if not report_type:
        raise HTTPException(status_code=400, detail="Не указан параметр 'report_type'")
    if report_type not in REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Неверный тип отчета. Доступны: {', '.join(REPORT_TYPES.keys())}"
        )

    file_path = Path(str(source_file).strip())

    try:
        # WORKAROUND для аномалии Amvera: асинхронное чтение с повтором
        # Причина: open() выбрасывает FileNotFoundError даже когда Path.exists() == True
        
        # Попытка 1: асинхронное чтение
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                file_content = await f.read()
            messages = await asyncio.to_thread(json.loads, file_content)
        except FileNotFoundError:
            # Попытка 2: Небольшая задержка + повторная попытка (возможна асинхронная синхронизация FS)
            await asyncio.sleep(0.1)
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                file_content = await f.read()
            messages = await asyncio.to_thread(json.loads, file_content)
        
        # MEMORY OPTIMIZATION: Освобождаем Page Cache после чтения
        release_page_cache(str(file_path))
        del file_content  # Явно удаляем строку из RAM

        if not isinstance(messages, list):
            raise HTTPException(status_code=400, detail="JSON-файл должен содержать массив")
        
        # Генерация данных через Gemini
        report_data = await generate_report_data(messages, report_type)
        
        # Генерация документа
        doc_buffer = generate_docx(report_data, report_type)
        
        # CRITICAL: Извлекаем байты и закрываем буфер СРАЗУ
        file_content = doc_buffer.getvalue()
        doc_buffer.close()
        del doc_buffer
        
        # Возврат документа
        filename = generate_filename(report_type)
        
        return Response(
            content=file_content,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except FileNotFoundError:
        # Если файл не найден, собираем МАКСИМУМ отладочной информации
        parent = file_path.parent
        debug_info = {
            "error_message": f"open() не смог найти файл: {file_path}",
            "file_path_exists": file_path.exists(),
            "file_is_file": file_path.is_file(),
            "parent_dir_exists": parent.exists(),
            "parent_dir_contents": None,
            "permissions": {
                "readable": os.access(file_path, os.R_OK),
                "parent_readable": os.access(parent, os.R_OK),
            }
        }
        try:
            if parent.exists():
                debug_info["parent_dir_contents"] = os.listdir(parent)
        except Exception as e:
            debug_info["parent_dir_contents"] = f"Ошибка чтения директории: {str(e)}"
            
        raise HTTPException(status_code=404, detail=debug_info)
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Некорректный JSON в файле")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Неизвестная ошибка: {str(e)}")


@router.post("/debug-file-system")
async def debug_file_system(data: dict = Body(...)):
    """Диагностика доступа к файловой системе."""
    import os
    from pathlib import Path

    path_to_check_str = data.get("path_to_check")
    if not path_to_check_str:
        raise HTTPException(status_code=400, detail="Не указан 'path_to_check'")

    path = Path(path_to_check_str)
    parent = path.parent

    debug_info = {
        "provided_path": str(path),
        "absolute_path": str(path.resolve()),
        "parent_directory": str(parent),
        "path_exists": path.exists(),
        "path_is_file": path.is_file(),
        "path_is_dir": path.is_dir(),
        "parent_exists": parent.exists(),
        "parent_is_dir": parent.is_dir(),
        "parent_contents": None,
        "permissions": {
            "path_readable": os.access(path, os.R_OK),
            "parent_readable": os.access(parent, os.R_OK),
            "parent_writable": os.access(parent, os.W_OK),
            "parent_executable": os.access(parent, os.X_OK),
        }
    }

    try:
        if parent.exists() and parent.is_dir():
            debug_info["parent_contents"] = os.listdir(parent)
    except Exception as e:
        debug_info["parent_contents"] = f"Ошибка чтения директории: {str(e)}"

    return JSONResponse(content=debug_info)


@router.post("/parse-and-generate")
async def parse_and_generate_report(data: dict = Body(...)):
    """
    Парсит Telegram-каналы и сразу генерирует отчет (всё в одном запросе).
    
    Ожидает JSON:
    {
        "days": 4,                // Архивный режим: за последние N дней
        "report_type": "news",    // Тип отчета: "news", "events", "custom_task_1", "custom_task_2"
        "provider_mode": "free"   // Режим LLM: "free" (Google) или "paid" (Polza.ai)
    }
    
    ИЛИ
    {
        "period": "today",        // Оперативный режим: с начала сегодняшнего дня
        "report_type": "news",
        "provider_mode": "paid"   // Использовать Polza.ai (платный режим)
    }
    
    Параметры:
    - provider_mode: "free" (по умолчанию) - прямой Google Gemini API
                     "paid" - через Polza.ai агрегатор
    
    Возвращает готовый .docx файл-отчет.
    """
    import json
    import os
    import time
    from pathlib import Path
    from services.telegram_parser import parse_telegram_channels, calculate_date_range
    
    # Валидация параметров
    report_type = data.get("report_type")
    days = data.get("days")
    period = data.get("period")
    channel_source = data.get("channel_source", "general")
    model_alias = data.get("model_alias", DEFAULT_MODEL_ALIAS)
    provider_mode = data.get("provider_mode", "free")  # "free" | "paid"
    
    # Валидация алиаса модели
    if model_alias not in MODEL_MAPPING:
        logger.warning(f"Неизвестный алиас модели: {model_alias}, используется {DEFAULT_MODEL_ALIAS}")
        model_alias = DEFAULT_MODEL_ALIAS
    
    # Валидация режима провайдера
    if provider_mode not in AVAILABLE_PROVIDER_MODES:
        logger.warning(f"Неизвестный режим провайдера: {provider_mode}, используется 'free'")
        provider_mode = "free"
    
    logger.info(f"Генерация отчета: period={period}, days={days}, type={report_type}, model={model_alias}, provider={provider_mode}")
    
    if not report_type:
        raise HTTPException(status_code=400, detail="Не указан параметр 'report_type'")
    
    if report_type not in REPORT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Неверный тип отчета. Доступны: {', '.join(REPORT_TYPES.keys())}"
        )
    
    if not days and not period:
        raise HTTPException(
            status_code=400,
            detail="Необходимо указать либо 'days', либо 'period'"
        )
    
    try:
        # Рассчитываем даты для парсинга
        start_date, end_date = calculate_date_range(days=days, period=period)
        
        # Определяем путь к файлу кэша
        base_path = "/data" if os.environ.get("AMVERA") else "data"
        
        # Суффикс по источнику (source_1 = без суффикса для обратной совместимости)
        suffix = "" if channel_source == "source_1" else f"_{channel_source}"

        if period == "today":
            cache_file = f"{base_path}/temp/today_cache{suffix}.json"
        elif period == "yesterday":
            # Вчерашняя дата для имени файла
            yesterday_date = start_date.strftime('%Y-%m-%d')
            cache_file = f"{base_path}/raw_parses/{yesterday_date}{suffix}.json"
        else:
            start_str = start_date.strftime('%Y-%m-%d')
            if days and days > 1:
                end_date_str = end_date.strftime('%Y-%m-%d')
                filename = f"{start_str}_to_{end_date_str}{suffix}.json"
            else:
                filename = f"{start_str}{suffix}.json"
            cache_file = f"{base_path}/raw_parses/{filename}"
        
        # Проверка кэша
        cache_path = Path(cache_file)
        use_cache = False
        if cache_path.exists() and cache_path.is_file():
            if period == "today":
                max_age = int(os.getenv("TODAY_CACHE_MAX_AGE_SEC", "300"))
                age = time.time() - cache_path.stat().st_mtime
                use_cache = age <= max_age
            else:
                use_cache = True

        if use_cache:
            logger.info(f"Используется кэш: {cache_file}")
            async with aiofiles.open(cache_file, 'r', encoding='utf-8') as f:
                content = await f.read()
            messages = await asyncio.to_thread(json.loads, content)
            # MEMORY OPTIMIZATION: Освобождаем Page Cache и строку
            release_page_cache(cache_file)
            del content
            logger.info(f"Загружено из кэша: {len(messages)} сообщений")
        else:
            # Парсинг должен выполняться строго последовательно (Telethon)
            async with PARSER_LOCK:
                # Повторная проверка кэша после ожидания lock (вдруг уже появился)
                if cache_path.exists() and cache_path.is_file():
                    if period == "today":
                        max_age = int(os.getenv("TODAY_CACHE_MAX_AGE_SEC", "300"))
                        age = time.time() - cache_path.stat().st_mtime
                        use_cache_after_wait = age <= max_age
                    else:
                        use_cache_after_wait = True
                else:
                    use_cache_after_wait = False

                if use_cache_after_wait:
                    logger.info(f"Используется кэш (после ожидания): {cache_file}")
                    async with aiofiles.open(cache_file, 'r', encoding='utf-8') as f:
                        content = await f.read()
                    messages = await asyncio.to_thread(json.loads, content)
                    # MEMORY OPTIMIZATION: Освобождаем Page Cache и строку
                    release_page_cache(cache_file)
                    del content
                    logger.info(f"Загружено из кэша: {len(messages)} сообщений")
                else:
                    if period != "today":
                        logger.info(f"Кэш не найден, выполняется парсинг...")
                    else:
                        logger.info(f"TODAY cache устарел/отсутствует, выполняется парсинг...")

                    # КРИТИЧНО: Telethon блокирует event loop — запускаем в отдельном потоке
                    messages = await asyncio.to_thread(
                        lambda: asyncio.run(parse_telegram_channels(start_date, end_date, channel_source=channel_source))
                    )
                    logger.info(f"Собрано сообщений: {len(messages)}")

                    # Сохраняем результат в файл
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    json_content = await asyncio.to_thread(
                        json.dumps, messages, ensure_ascii=False, indent=2
                    )
                    async with aiofiles.open(cache_file, 'w', encoding='utf-8') as f:
                        await f.write(json_content)
                    logger.info(f"Результат сохранен в: {cache_file}")
        
        if not messages:
            raise HTTPException(
                status_code=404,
                detail="За указанный период не найдено ни одного сообщения"
            )
        
        # Генерация данных для отчета через LLM провайдер
        logger.info(f"Selected model: {model_alias}, provider: {provider_mode}")
        report_data = await generate_report_data(
            messages, 
            report_type, 
            model_alias=model_alias,
            provider_mode=provider_mode
        )
        
        # CRITICAL: Очистка messages из RAM СРАЗУ после generate_report_data
        # ✅ Файл на диске остается нетронутым!
        messages_size_mb = len(str(messages).encode('utf-8')) / (1024 * 1024)
        logger.info(f"EXPLICIT CLEANUP (RAM): Clearing messages after report (~{messages_size_mb:.1f}MB)")
        logger.info(f"✅ File on disk is safe: {cache_file}")
        messages.clear()
        del messages
        
        # Генерация .docx документа (CPU/IO bound) в отдельном потоке
        doc_buffer = await asyncio.to_thread(generate_docx, report_data, report_type)
        
        # CRITICAL: Извлекаем байты и закрываем буфер СРАЗУ
        file_content = doc_buffer.getvalue()
        doc_buffer.close()
        del doc_buffer
        
        # Очищаем report_data после генерации документа
        del report_data
        
        # Возврат документа
        filename = generate_filename(report_type)
        logger.info(f"Отчет готов: {filename}")
        
        return Response(
            content=file_content,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка: {type(e).__name__}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при генерации отчета: {str(e)}"
        )


@router.get("/types")
async def get_report_types():
    """Возвращает список доступных типов отчетов и режимов провайдеров."""
    return JSONResponse(content={
        "available_types": list(REPORT_TYPES.keys()),
        "descriptions": {
            "news": "Дайджест новостей",
            "events": "Календарь мероприятий",
            "custom_task_1": "Доп. сценарий",
            "custom_task_2": "Доп. сценарий 2"
        },
        "available_models": list(MODEL_MAPPING.keys()),
        "available_provider_modes": list(AVAILABLE_PROVIDER_MODES),
        "provider_mode_descriptions": {
            "free": "Google Gemini API (бесплатно)",
            "paid": "Polza.ai агрегатор (платно, в рублях)"
        }
    })

