"""
Сервис для генерации отчетов через LLM провайдеры.

Поддерживает два режима:
- free: прямой доступ к Google Gemini API
- paid: доступ через Polza.ai агрегатор
"""
import json
import io
import os
import gc
import re
import logging
import asyncio
import aiofiles
from datetime import datetime
from docxtpl import DocxTemplate
import pytils

from services.llm_providers import (
    get_provider,
    MODEL_MAPPING,
    DEFAULT_MODEL_ALIAS
)

# Настройка логирования
logger = logging.getLogger(__name__)

# Маппинг типов отчетов
# Маппинг типов отчетов на файлы промптов
# news, events — стандартные отчеты
# custom_task_1, custom_task_2 — дополнительные сценарии (настраиваемые)
REPORT_TYPES = {
    "news": "news.md",
    "events": "events.md",
    "custom_task_1": "custom_task_1.md",
    "custom_task_2": "custom_task_2.md"
}

# Реэкспорт для обратной совместимости с routers/reports.py
AVAILABLE_MODELS = {alias: mapping["google"] for alias, mapping in MODEL_MAPPING.items()}


def sanitize_json_response(raw: str) -> str:
    """
    Очищает ответ LLM от markdown-обёрток.
    
    Некоторые модели (особенно через Polza.ai) могут возвращать JSON
    обёрнутый в markdown code blocks: ```json ... ```
    
    Эта функция:
    1. Удаляет markdown code blocks (```json ... ``` или ``` ... ```)
    2. Извлекает JSON от первой { до последней }
    
    :param raw: Сырой ответ от LLM
    :return: Очищенная JSON-строка
    """
    if not raw:
        return raw
    
    text = raw.strip()
    
    # 1. Удаляем markdown code blocks: ```json ... ``` или ``` ... ```
    # Паттерн для начала блока (с опциональным "json" и переносом строки)
    text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.IGNORECASE)
    # Паттерн для конца блока
    text = re.sub(r'\n?```\s*$', '', text)
    
    # 2. Fallback: извлекаем от первой { до последней }
    # Это защита на случай, если есть текст до/после JSON
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    
    if first_brace != -1 and last_brace > first_brace:
        text = text[first_brace:last_brace + 1]
    
    return text.strip()


async def read_prompt(report_type: str) -> str:
    """Читает промпт из файла в зависимости от типа отчета (async)."""
    if report_type not in REPORT_TYPES:
        raise ValueError(f"Неизвестный тип отчета: {report_type}")
    
    prompt_filename = REPORT_TYPES[report_type]
    # Промпты находятся в коде приложения, не в /data/
    # Локально: prompts/news.md
    # На Amvera: /app/prompts/news.md (или просто prompts/news.md от рабочей директории /app/)
    prompt_path = f"prompts/{prompt_filename}"
    
    try:
        async with aiofiles.open(prompt_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            return content.strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"Промпт не найден: {prompt_path}")


def prepare_telegram_data(messages: list[dict]) -> str:
    """
    Объединяет сообщения в один большой текст, 
    сохраняя метаданные для каждого сообщения в виде "карточки".
    
    MEMORY OPTIMIZATION: Использует генератор для избежания
    создания промежуточного списка строк в RAM.
    
    :param messages: Список сообщений из парсера.
    :return: Объединенный текст с метаданными.
    """
    def generate_blocks():
        """Генератор блоков сообщений - не создает список в памяти"""
        has_messages = False
        for item in messages:
            if isinstance(item, dict) and "text" in item:
                has_messages = True
                # Извлекаем все нужные данные с безопасными значениями по умолчанию
                channel = item.get("channel_name", "N/A")
                date = item.get("publication_date", "N/A")
                msg_id = item.get("message_id", "N/A")
                text = item.get("text", "")

                # Формируем "карточку" для каждого сообщения
                yield (
                    f"--- НАЧАЛО СООБЩЕНИЯ ---\n"
                    f"Источник: {channel}\n"
                    f"Дата: {date}\n"
                    f"ID Сообщения: {msg_id}\n\n"
                    f"{text.strip()}\n"
                    f"--- КОНЕЦ СООБЩЕНИЯ ---"
                )
        
        if not has_messages:
            raise ValueError("В данных не найдено ни одного текстового сообщения")
    
    # Склеиваем блоки через генератор (join принимает итератор)
    return "\n\n\n".join(generate_blocks())


async def generate_report_data(
    telegram_messages: list[dict], 
    report_type: str, 
    model_alias: str = None,
    provider_mode: str = "free"
) -> dict:
    """
    Отправляет данные в LLM и получает структурированный JSON.
    
    :param telegram_messages: Список сообщений из Telegram.
    :param report_type: Тип отчета (news, events, custom_task_1, custom_task_2).
    :param model_alias: Алиас модели (flash_2_5 или flash_3_0).
    :param provider_mode: Режим провайдера ("free" или "paid").
    :return: Структурированные данные от LLM.
    """
    # Определяем модель
    if model_alias is None:
        model_alias = DEFAULT_MODEL_ALIAS
    
    # Получаем провайдер через фабрику
    provider = get_provider(provider_mode=provider_mode, model_alias=model_alias)
    
    logger.info(f"Используется провайдер: {provider.provider_name}, модель: {provider.model_id} (alias: {model_alias})")
    
    # Подготовка данных (в отдельном потоке, чтобы не блокировать event loop)
    telegram_text = await asyncio.to_thread(prepare_telegram_data, telegram_messages)
    
    # CRITICAL OPTIMIZATION: Очистка telegram_messages из RAM СРАЗУ после prepare
    # Файл на диске (если был) НЕ ТРОНУТ!
    if telegram_messages:
        messages_size_mb = sum(len(str(m).encode('utf-8')) for m in telegram_messages) / (1024 * 1024)
        logger.info(f"EXPLICIT CLEANUP (RAM): Clearing telegram_messages (~{messages_size_mb:.1f}MB)")
        telegram_messages.clear()  # Очищаем список
        del telegram_messages      # Удаляем ссылку
    
    system_prompt = await read_prompt(report_type)
    
    # Динамически вставляем текущую дату в промпт для корректной сортировки
    if "[ДАТА ФОРМИРОВАНИЯ ОТЧЕТА]" in system_prompt:
        today_str = datetime.now().strftime('%Y-%m-%d')
        system_prompt = system_prompt.replace("[ДАТА ФОРМИРОВАНИЯ ОТЧЕТА]", today_str)
    
    raw_response = None
    clean_response = None
    try:
        # Вызов провайдера (абстрагирует разницу между Google и Polza.ai)
        raw_response = await provider.generate(system_prompt, telegram_text)
        
        # MEMORY OPTIMIZATION: Очистка больших строк сразу после использования
        text_size_mb = len(telegram_text.encode('utf-8')) / (1024 * 1024)
        logger.info(f"EXPLICIT CLEANUP (RAM): Clearing telegram_text after API (~{text_size_mb:.1f}MB)")
        del telegram_text
        del system_prompt  # Промпт тоже больше не нужен
        
        # Очистка от markdown-обёрток (```json ... ```)
        # Работает для любого провайдера, защита от markdown в ответе
        clean_response = sanitize_json_response(raw_response)
        
        # Парсинг ответа (асинхронно, чтобы не блокировать event loop)
        report_data = await asyncio.to_thread(json.loads, clean_response)
        
        # MEMORY OPTIMIZATION: Очистка после успешного парсинга
        del raw_response
        del clean_response
        raw_response = None
        clean_response = None
        
        # Добавляем РЕАЛЬНУЮ дату формирования отчета
        today = datetime.now().strftime('%d.%m.%Y')
        report_data['generation_date'] = today
        
        return report_data
        
    except json.JSONDecodeError as e:
        # Фолбэк: ответ оборван. Пытаемся извлечь все полностью сформированные JSON-объекты
        # из массива "items" и построить минимально валидный результат.
        # Сначала очищаем от markdown-обёрток
        raw_text = sanitize_json_response(raw_response) if raw_response else ""

        def extract_items_from_truncated_json(text: str) -> list[dict]:
            # Ищем начало массива items
            key_pos = text.find('"items"')
            if key_pos == -1:
                return []
            bracket_pos = text.find('[', key_pos)
            if bracket_pos == -1:
                return []

            i = bracket_pos + 1
            n = len(text)
            in_string = False
            escape = False
            brace_depth = 0
            current_start = None
            results: list[dict] = []

            while i < n:
                ch = text[i]

                if in_string:
                    if escape:
                        escape = False
                    elif ch == '\\':
                        escape = True
                    elif ch == '"':
                        in_string = False
                    i += 1
                    continue

                # вне строки
                if ch == '"':
                    in_string = True
                    i += 1
                    continue

                if ch == '{':
                    if brace_depth == 0:
                        current_start = i
                    brace_depth += 1
                elif ch == '}':
                    if brace_depth > 0:
                        brace_depth -= 1
                        if brace_depth == 0 and current_start is not None:
                            obj_text = text[current_start:i+1]
                            try:
                                obj = json.loads(obj_text)
                                results.append(obj)
                            except json.JSONDecodeError:
                                pass  # частично битый объект — пропускаем
                            current_start = None
                elif ch == ']':
                    # Закрытие массива items на верхнем уровне (вне объектов)
                    if brace_depth == 0:
                        break

                i += 1

            return results

        parsed_items = extract_items_from_truncated_json(raw_text)
        if not parsed_items:
            error_text = raw_text[:500] if raw_text else "No response"
            # MEMORY OPTIMIZATION: Очистка перед raise
            del raw_text
            raise ValueError(
                f"LLM вернул некорректный JSON, не удалось извлечь объекты: {error_text}"
            )

        # MEMORY OPTIMIZATION: raw_text больше не нужен
        del raw_text

        # Формируем минимально валидную структуру отчета на основе частично извлеченных данных
        report_data = {
            "items": parsed_items
        }

        # Добавляем дату формирования отчета
        today = datetime.now().strftime('%d.%m.%Y')
        report_data['generation_date'] = today

        return report_data
    finally:
        # Гарантированная очистка переменных в случае ошибки
        # (в успешном пути они уже удалены через del)
        pass


def generate_docx(report_data: dict, report_type: str) -> io.BytesIO:
    """
    Генерирует .docx документ из данных.
    
    MEMORY OPTIMIZATION: Явная очистка внутренних структур DocxTemplate
    после сохранения для предотвращения удержания памяти.
    
    :param report_data: Данные от Gemini.
    :param report_type: Тип отчета.
    :return: BytesIO с содержимым документа.
    """
    # Выбор шаблона в зависимости от типа отчета
    # Маппинг типов отчетов на файлы шаблонов .docx
    template_mapping = {
        "news": "templates/news.docx",
        "events": "templates/events.docx",
        "custom_task_1": "templates/custom_task_1.docx",
        "custom_task_2": "templates/custom_task_2.docx"
    }
    
    template_path = template_mapping.get(report_type)
    if not template_path:
        raise ValueError(f"Неизвестный тип отчета: {report_type}")
    
    try:
        doc = DocxTemplate(template_path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Шаблон не найден: {template_path}. "
            f"Создайте шаблон согласно инструкции в TEMPLATE_GUIDE.md"
        )
    except Exception as e:
        raise FileNotFoundError(f"Не удалось открыть шаблон {template_path}: {e}")
    
    # Рендеринг
    doc.render(report_data)
    
    # Сохранение в буфер
    doc_buffer = io.BytesIO()
    doc.save(doc_buffer)
    doc_buffer.seek(0)
    
    # MEMORY OPTIMIZATION: Явная очистка внутренних структур DocxTemplate
    # DocxTemplate держит python-docx Document и jinja2 Environment
    try:
        # Очистка внутреннего документа python-docx
        if hasattr(doc, 'docx') and doc.docx is not None:
            doc.docx = None
        # Очистка шаблона jinja2
        if hasattr(doc, '_tpl') and doc._tpl is not None:
            doc._tpl = None
        # Очистка среды jinja2
        if hasattr(doc, 'jinja_env') and doc.jinja_env is not None:
            doc.jinja_env = None
        # Очистка кэша
        if hasattr(doc, 'crc_to_new_media'):
            doc.crc_to_new_media.clear() if doc.crc_to_new_media else None
        if hasattr(doc, 'crc_to_new_embedded'):
            doc.crc_to_new_embedded.clear() if doc.crc_to_new_embedded else None
    except Exception:
        pass  # Безопасно игнорируем ошибки очистки
    
    del doc
    
    return doc_buffer


def generate_filename(report_type: str) -> str:
    """
    Генерирует безопасное транслитерированное имя файла.
    
    :param report_type: Тип отчета.
    :return: Имя файла.
    """
    # Названия типов отчетов для формирования имени файла
    type_names = {
        "news": "новостям",
        "events": "мероприятиям",
        "custom_task_1": "сценарию_1",
        "custom_task_2": "сценарию_2"
    }
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    original_name = f"Отчет_по_{type_names.get(report_type, 'данным')}_{date_str}.docx"
    
    return pytils.translit.translify(original_name)

