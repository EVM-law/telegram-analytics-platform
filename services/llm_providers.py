"""
LLM провайдеры для генерации отчетов.

Паттерн Стратегия: единый интерфейс для разных LLM API.
- GeminiProvider: прямой доступ к Google Gemini API (бесплатный режим)
- PolzaProvider: доступ через Polza.ai агрегатор (платный режим)
"""
import os
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from openai import OpenAI
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Единый маппинг алиасов на ID моделей для обоих провайдеров
MODEL_MAPPING = {
    "flash_2_5": {
        "google": "gemini-2.5-flash",
        "polza": "google/gemini-2.5-flash"
    },
    "flash_3_0": {
        "google": "gemini-3-flash-preview",
        "polza": "google/gemini-3-flash-preview"
    }
}

DEFAULT_MODEL_ALIAS = "flash_2_5"

# Polza.ai конфигурация
POLZA_API_URL = os.environ.get("POLZA_API_URL", "https://api.polza.ai/api/v1")
POLZA_API_KEY = os.environ.get("POLZA_API_KEY", "")

# Google Gemini конфигурация
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY", "")


# ============================================================================
# СИНГЛТОНЫ КЛИЕНТОВ
# ============================================================================

_gemini_client: Optional[genai.Client] = None


def get_gemini_client() -> genai.Client:
    """Получить синглтон Gemini client"""
    global _gemini_client
    if _gemini_client is None:
        if not GEMINI_API_KEY:
            raise ValueError("GOOGLE_API_KEY environment variable is not set")
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
        logger.info("Gemini client singleton created")
    return _gemini_client


# ============================================================================
# БАЗОВЫЙ КЛАСС ПРОВАЙДЕРА
# ============================================================================

class BaseLLMProvider(ABC):
    """Абстрактный провайдер для LLM API"""
    
    def __init__(self, model_alias: str = None):
        self.model_alias = model_alias or DEFAULT_MODEL_ALIAS
        if self.model_alias not in MODEL_MAPPING:
            logger.warning(f"Неизвестный алиас модели: {self.model_alias}, используется {DEFAULT_MODEL_ALIAS}")
            self.model_alias = DEFAULT_MODEL_ALIAS
    
    @abstractmethod
    async def generate(self, system_prompt: str, user_content: str) -> str:
        """
        Генерирует ответ от LLM.
        
        :param system_prompt: Системный промпт (инструкции)
        :param user_content: Контент пользователя (данные для анализа)
        :return: Raw text ответа (JSON string)
        """
        pass
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Имя провайдера для логирования"""
        pass
    
    @property
    @abstractmethod
    def model_id(self) -> str:
        """ID модели для текущего провайдера"""
        pass


# ============================================================================
# GOOGLE GEMINI PROVIDER (FREE MODE)
# ============================================================================

class GeminiProvider(BaseLLMProvider):
    """
    Провайдер для прямого доступа к Google Gemini API.
    Используется в бесплатном режиме.
    """
    
    @property
    def provider_name(self) -> str:
        return "Google Gemini"
    
    @property
    def model_id(self) -> str:
        return MODEL_MAPPING[self.model_alias]["google"]
    
    async def generate(self, system_prompt: str, user_content: str) -> str:
        """
        Генерирует ответ через Google Gemini API.
        
        Использует google-genai библиотеку с синхронным API,
        обернутым в asyncio.to_thread для неблокирующего вызова.
        
        MEMORY OPTIMIZATION: Явная очистка contents и response после использования.
        """
        client = get_gemini_client()
        
        # Gemini формат: contents как список строк
        contents = [system_prompt, user_content]
        
        # Конфигурация генерации
        config = types.GenerateContentConfig(
            top_p=1,
            top_k=40,
            max_output_tokens=65536,
            temperature=0.3,
            thinking_config=types.ThinkingConfig(thinking_budget=24576),
            response_mime_type="application/json"
        )
        
        logger.info(f"LLM Request: provider={self.provider_name}, model={self.model_id}")
        
        # Вызов синхронного Gemini API в отдельном потоке
        response = await asyncio.to_thread(
            lambda: client.models.generate_content(
                model=self.model_id,
                contents=contents,
                config=config
            )
        )
        
        logger.info(f"LLM Response: provider={self.provider_name}, model={self.model_id}")
        
        # MEMORY OPTIMIZATION: Извлекаем текст и очищаем большие объекты
        result_text = response.text
        
        # Очищаем contents (большие строки) - они больше не нужны
        contents.clear()
        del contents
        
        # Очищаем response object от google-genai
        del response
        
        return result_text


# ============================================================================
# POLZA.AI PROVIDER (PAID MODE)
# ============================================================================

# Синглтон клиента Polza.ai (OpenAI SDK)
_polza_client: Optional[OpenAI] = None


def get_polza_client() -> OpenAI:
    """Получить синглтон Polza.ai client (OpenAI SDK)"""
    global _polza_client
    if _polza_client is None:
        if not POLZA_API_KEY:
            raise ValueError("POLZA_API_KEY environment variable is not set")
        _polza_client = OpenAI(
            base_url=POLZA_API_URL,
            api_key=POLZA_API_KEY
        )
        logger.info(f"Polza.ai OpenAI client created (base_url={POLZA_API_URL})")
    return _polza_client


class PolzaProvider(BaseLLMProvider):
    """
    Провайдер для доступа к LLM через Polza.ai агрегатор.
    Используется в платном режиме.
    
    Polza.ai использует OpenAI-совместимый протокол.
    Согласно документации: https://docs.polza.ai/docs/api-spravochnik/textovaya-generaciya/post-chat-completion
    """
    
    def __init__(self, model_alias: str = None):
        super().__init__(model_alias)
        if not POLZA_API_KEY:
            raise ValueError("POLZA_API_KEY environment variable is not set for paid mode")
    
    @property
    def provider_name(self) -> str:
        return "Polza.ai"
    
    @property
    def model_id(self) -> str:
        return MODEL_MAPPING[self.model_alias]["polza"]
    
    async def generate(self, system_prompt: str, user_content: str) -> str:
        """
        Генерирует ответ через Polza.ai API (OpenAI-compatible).
        
        Использует официальный OpenAI SDK согласно рекомендациям Polza.ai.
        
        Параметры:
        - reasoning_effort: OpenAI-совместимый параметр для thinking mode
          Маппинг: "high" → thinking_budget=24576 (как в GeminiProvider)
          Документация: https://ai.google.dev/gemini-api/docs/openai
        - Очистка markdown-обёрток в report_generator.sanitize_json_response()
        
        MEMORY OPTIMIZATION: Явная очистка messages и response после использования.
        
        Документация Polza.ai: https://docs.polza.ai/docs/api-spravochnik/textovaya-generaciya/post-chat-completion
        """
        client = get_polza_client()
        
        # OpenAI формат: messages с ролями
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ]
        
        logger.info(f"LLM Request: provider={self.provider_name}, model={self.model_id}")
        
        # Вызов синхронного OpenAI SDK в отдельном потоке
        # reasoning_effort - стандартный OpenAI параметр для thinking
        # "high" = thinking_budget 24576 (как в GeminiProvider)
        # Документация: https://ai.google.dev/gemini-api/docs/openai
        response = await asyncio.to_thread(
            lambda: client.chat.completions.create(
                model=self.model_id,
                messages=messages,
                max_tokens=65536,
                temperature=0.3,
                top_p=1.0,
                reasoning_effort="high"
            )
        )
        
        # Извлечение контента
        content = response.choices[0].message.content
        
        # Логирование usage и стоимости
        if response.usage:
            total_tokens = response.usage.total_tokens
            # Polza.ai добавляет cost в usage (нестандартное поле)
            cost = getattr(response.usage, 'cost', None)
            
            logger.info(f"LLM Response: provider={self.provider_name}, tokens={total_tokens}")
            if cost:
                logger.info(f"Polza.ai cost: {cost} RUB")
        else:
            logger.info(f"LLM Response: provider={self.provider_name}")
        
        # MEMORY OPTIMIZATION: Очищаем большие объекты
        messages.clear()
        del messages
        del response
        
        return content


# ============================================================================
# ФАБРИКА ПРОВАЙДЕРОВ
# ============================================================================

def get_provider(provider_mode: str = "free", model_alias: str = None) -> BaseLLMProvider:
    """
    Фабрика для создания провайдера LLM.
    
    :param provider_mode: Режим провайдера ("free" или "paid")
    :param model_alias: Алиас модели (flash_2_5 или flash_3_0)
    :return: Экземпляр провайдера
    """
    if provider_mode == "paid":
        logger.info(f"Creating PolzaProvider (paid mode), model_alias={model_alias}")
        return PolzaProvider(model_alias)
    else:
        logger.info(f"Creating GeminiProvider (free mode), model_alias={model_alias}")
        return GeminiProvider(model_alias)

