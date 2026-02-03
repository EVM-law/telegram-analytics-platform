"""
API endpoints для парсинга Telegram-каналов
"""
import os
from datetime import datetime
from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import JSONResponse
from services.telegram_parser import parse_telegram_channels, calculate_date_range


def release_page_cache(file_path: str) -> None:
    """
    Освобождает Page Cache для файла после его чтения.
    Файл остается на диске, но выгружается из RAM ядра Linux.
    """
    try:
        POSIX_FADV_DONTNEED = 4
        fd = os.open(file_path, os.O_RDONLY)
        try:
            file_size = os.fstat(fd).st_size
            os.posix_fadvise(fd, 0, file_size, POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    except (AttributeError, OSError):
        pass  # Windows или ошибка FS - безопасно игнорируем


router = APIRouter(prefix="/api/parser", tags=["parser"])


@router.post("/parse")
async def parse_channels(data: dict = Body(...)):
    """
    Парсит Telegram-каналы и возвращает собранные данные.
    
    Ожидает JSON:
    {
        "days": 3  // Архивный режим: за последние N дней
    }
    ИЛИ
    {
        "period": "today"  // Оперативный режим: с начала сегодняшнего дня
    }
    
    Также можно указать точные даты:
    {
        "start_date": "2025-10-08T00:00:00+03:00",
        "end_date": "2025-10-09T00:00:00+03:00"
    }
    """
    try:
        # Вариант 1: Точные даты
        if "start_date" in data and "end_date" in data:
            start_date = datetime.fromisoformat(data["start_date"])
            end_date = datetime.fromisoformat(data["end_date"])
        
        # Вариант 2: days или period
        elif "days" in data or "period" in data:
            days = data.get("days")
            period = data.get("period")
            start_date, end_date = calculate_date_range(days=days, period=period)
        
        else:
            raise HTTPException(
                status_code=400,
                detail="Необходимо указать либо (start_date, end_date), либо (days), либо (period='today')"
            )
        
        # Парсинг
        messages = await parse_telegram_channels(start_date, end_date)
        
        return JSONResponse(content={
            "success": True,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_messages": len(messages),
            "messages": messages
        })
    
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")


@router.post("/parse-and-save")
async def parse_and_save(data: dict = Body(...)):
    """
    Парсит Telegram-каналы и сохраняет результат в JSON-файл.
    
    Ожидает JSON:
    {
        "days": 3,
        "output_file": "/data/raw_parses/2025-10-07.json"  // опционально (абсолютный путь для Amvera)
    }
    
    Примечание: На Amvera используйте абсолютный путь /data/... для персистентного хранения
    """
    try:
        import json
        from pathlib import Path
        
        # Рассчитываем даты
        days = data.get("days")
        period = data.get("period")
        
        if "start_date" in data and "end_date" in data:
            start_date = datetime.fromisoformat(data["start_date"])
            end_date = datetime.fromisoformat(data["end_date"])
        else:
            start_date, end_date = calculate_date_range(days=days, period=period)
        
        # Определяем путь для сохранения
        output_file = data.get("output_file")
        if not output_file:
            # Базовый путь в зависимости от окружения
            base_path = "/data" if os.environ.get("AMVERA") else "data"
            
            # Режим "today" — временный файл в папке temp/
            if period == "today":
                output_file = f"{base_path}/temp/today_cache.json"
            else:
                # Архивный режим — папка raw_parses/
                start_str = start_date.strftime('%Y-%m-%d')
                
                # Если период > 1 дня, используем формат start_to_end
                if days and days > 1:
                    end_date_str = end_date.strftime('%Y-%m-%d')
                    filename = f"{start_str}_to_{end_date_str}.json"
                else:
                    filename = f"{start_str}.json"
                
                output_file = f"{base_path}/raw_parses/{filename}"
        
        # Проверка кэша для архивных данных (не для "today")
        output_path = Path(output_file)
        if period != "today" and output_path.exists() and output_path.is_file():
            # Файл уже существует, используем его
            print(f"Кэш найден: {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            # MEMORY OPTIMIZATION: Освобождаем Page Cache (файл остается на диске)
            release_page_cache(output_file)
            print(f"Загружено из кэша: {len(messages)} сообщений")
        else:
            # Выполняем парсинг
            if period != "today":
                print(f"Кэш не найден, выполняется парсинг...")
            
            messages = await parse_telegram_channels(start_date, end_date)
            
            # Создаём директории
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Сохраняем
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            # MEMORY OPTIMIZATION: Освобождаем Page Cache после записи
            release_page_cache(output_file)
            print(f"Результат сохранен в: {output_file}")
        
        return JSONResponse(content={
            "success": True,
            "output_file": output_file,
            "total_messages": len(messages),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения: {str(e)}")


@router.get("/list-files")
async def list_parsed_files():
    """
    Диагностический endpoint: показывает все файлы в директории /data/raw_parses/
    
    Используйте для проверки, какие файлы реально доступны приложению.
    """
    try:
        import os
        from pathlib import Path
        
        # Определяем базовый путь
        base_path = "/data" if os.environ.get("AMVERA") else "data"
        raw_parses_dir = Path(base_path) / "raw_parses"
        temp_dir = Path(base_path) / "temp"
        
        result = {
            "raw_parses_directory": str(raw_parses_dir),
            "temp_directory": str(temp_dir),
            "raw_parses_exists": raw_parses_dir.exists(),
            "temp_exists": temp_dir.exists(),
            "raw_parses_files": [],
            "temp_files": []
        }
        
        # Список файлов в raw_parses
        if raw_parses_dir.exists():
            result["raw_parses_files"] = [
                {
                    "filename": f.name,
                    "full_path": str(f),
                    "size_bytes": f.stat().st_size,
                    "modified": f.stat().st_mtime
                }
                for f in raw_parses_dir.iterdir() if f.is_file()
            ]
        
        # Список файлов в temp
        if temp_dir.exists():
            result["temp_files"] = [
                {
                    "filename": f.name,
                    "full_path": str(f),
                    "size_bytes": f.stat().st_size,
                    "modified": f.stat().st_mtime
                }
                for f in temp_dir.iterdir() if f.is_file()
            ]
        
        return JSONResponse(content=result)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

