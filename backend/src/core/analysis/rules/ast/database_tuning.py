import json
from typing import Dict, List, Any, Optional, Set
from ....models.lint_diagnose import LintDiagnose


def rule_database_tuning(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Рекомендации по настройке параметров базы данных
    """
    recommendations = []

    try:
        # Анализ настроек базы данных
        db_settings = context.get('settings', {})

        # Рекомендации по work_mem
        work_mem_recommendations = analyze_work_mem_settings(db_settings, context)
        recommendations.extend(work_mem_recommendations)

        # Рекомендации по shared_buffers
        shared_buffers_recommendations = analyze_shared_buffers_settings(db_settings, context)
        recommendations.extend(shared_buffers_recommendations)

        # Рекомендации по effective_cache_size
        effective_cache_size_recommendations = analyze_effective_cache_size_settings(db_settings, context)
        recommendations.extend(effective_cache_size_recommendations)

        # Рекомендации по maintenance_work_mem
        maintenance_work_mem_recommendations = analyze_maintenance_work_mem_settings(db_settings, context)
        recommendations.extend(maintenance_work_mem_recommendations)

        # Рекомендации по random_page_cost
        random_page_cost_recommendations = analyze_random_page_cost_settings(db_settings, context)
        recommendations.extend(random_page_cost_recommendations)

        # Рекомендации по seq_page_cost
        seq_page_cost_recommendations = analyze_seq_page_cost_settings(db_settings, context)
        recommendations.extend(seq_page_cost_recommendations)

        # Рекомендации по effective_io_concurrency
        effective_io_concurrency_recommendations = analyze_effective_io_concurrency_settings(db_settings, context)
        recommendations.extend(effective_io_concurrency_recommendations)

        # Рекомендации по checkpoint_segments
        checkpoint_segments_recommendations = analyze_checkpoint_segments_settings(db_settings, context)
        recommendations.extend(checkpoint_segments_recommendations)

        # Рекомендации по checkpoint_completion_target
        checkpoint_completion_target_recommendations = analyze_checkpoint_completion_target_settings(db_settings, context)
        recommendations.extend(checkpoint_completion_target_recommendations)

        # Рекомендации по wal_buffers
        wal_buffers_recommendations = analyze_wal_buffers_settings(db_settings, context)
        recommendations.extend(wal_buffers_recommendations)

        # Рекомендации по max_connections
        max_connections_recommendations = analyze_max_connections_settings(db_settings, context)
        recommendations.extend(max_connections_recommendations)

        # Рекомендации по ssl
        ssl_recommendations = analyze_ssl_settings(db_settings, context)
        recommendations.extend(ssl_recommendations)

    except Exception as e:
        print(f"Error in database_tuning rule: {e}")

    return recommendations


def analyze_work_mem_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек work_mem
    """
    recommendations = []

    try:
        work_mem = db_settings.get('work_mem', {}).get('setting', '4MB')

        # Парсинг значения work_mem
        work_mem_mb = parse_memory_value(work_mem)

        # Если work_mem слишком маленький
        if work_mem_mb < 16:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"work_mem установлен в {work_mem}, что может быть недостаточно для сложных запросов",
                recommendation="Рассуйте увеличение work_mem до 16-64MB для улучшения производительности сложных запросов и сортировок"
            ))

        # Если work_mem слишком большой
        elif work_mem_mb > 256:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"work_mem установлен в {work_mem}, что может потреблять слишком много памяти",
                recommendation="Рассуйте уменьшение work_mem до 64-128MB для предотвращения нехватки памяти при высокой нагрузке"
            ))

        # Анализ статистики для выявления необходимости изменения work_mem
        table_stats = context.get('table_stats', {})
        large_tables = [t for t, s in table_stats.items() if parse_memory_value(s.get('table_size', '0')) > 100]

        if large_tables and work_mem_mb < 32:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"Обнаружены большие таблицы ({len(large_tables)}), но work_mem установлен в {work_mem}",
                recommendation="Рассуйте увеличение work_mem до 32-64MB для работы с большими таблицами"
            ))

    except Exception as e:
        print(f"Error analyzing work_mem settings: {e}")

    return recommendations


def analyze_shared_buffers_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек shared_buffers
    """
    recommendations = []

    try:
        shared_buffers = db_settings.get('shared_buffers', {}).get('setting', '128MB')

        # Парсинг значения shared_buffers
        shared_buffers_mb = parse_memory_value(shared_buffers)

        # Оптимальное значение shared_buffers обычно 25% от доступной RAM
        # В реальном приложении нужно получить информацию о доступной памяти
        # Пока используем эвристику
        if shared_buffers_mb < 512:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="HIGH",
                message=f"shared_buffers установлен в {shared_buffers}, что может быть недостаточно для хорошей производительности",
                recommendation="Рассуйте увеличение shared_buffers до 25% от доступной RAM (обычно 1-4GB) для улучшения кэширования"
            ))

        elif shared_buffers_mb > 8192:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"shared_buffers установлен в {shared_buffers}, что может потреблять слишком много памяти",
                recommendation="Рассуйте уменьшение shared_buffers до 25% от доступной RAM для предотвращения нехватки памяти"
            ))

    except Exception as e:
        print(f"Error analyzing shared_buffers settings: {e}")

    return recommendations


def analyze_effective_cache_size_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек effective_cache_size
    """
    recommendations = []

    try:
        effective_cache_size = db_settings.get('effective_cache_size', {}).get('setting', '4GB')

        # Парсинг значения effective_cache_size
        effective_cache_size_mb = parse_memory_value(effective_cache_size)

        # Если effective_cache_size слишком маленький
        if effective_cache_size_mb < 2048:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"effective_cache_size установлен в {effective_cache_size}, что может быть недостаточно",
                recommendation="Рассуйте увеличение effective_cache_size до 4-8GB для улучшения планирования запросов"
            ))

        # Если effective_cache_size слишком большой
        elif effective_cache_size_mb > 16384:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"effective_cache_size установлен в {effective_cache_size}, что может быть завышен",
                recommendation="Рассуйте уменьшение effective_cache_size до 8-16GB для более реалистичного планирования"
            ))

    except Exception as e:
        print(f"Error analyzing effective_cache_size settings: {e}")

    return recommendations


def analyze_maintenance_work_mem_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек maintenance_work_mem
    """
    recommendations = []

    try:
        maintenance_work_mem = db_settings.get('maintenance_work_mem', {}).get('setting', '64MB')

        # Парсинг значения maintenance_work_mem
        maintenance_work_mem_mb = parse_memory_value(maintenance_work_mem)

        # Если maintenance_work_mem слишком маленький
        if maintenance_work_mem_mb < 128:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"maintenance_work_mem установлен в {maintenance_work_mem}, что может замедлять операции обслуживания",
                recommendation="Рассуйте увеличение maintenance_work_mem до 256-512MB для ускорения VACUUM, REINDEX и CREATE INDEX"
            ))

        # Если maintenance_work_mem слишком большой
        elif maintenance_work_mem_mb > 2048:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"maintenance_work_mem установлен в {maintenance_work_mem}, что может потреблять слишком много памяти",
                recommendation="Рассуйте уменьшение maintenance_work_mem до 512-1024MB для предотвращения нехватки памяти"
            ))

    except Exception as e:
        print(f"Error analyzing maintenance_work_mem settings: {e}")

    return recommendations


def analyze_random_page_cost_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек random_page_cost
    """
    recommendations = []

    try:
        random_page_cost = float(db_settings.get('random_page_cost', {}).get('setting', '1.1'))

        # Если random_page_cost слишком маленький
        if random_page_cost < 1.0:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"random_page_cost установлен в {random_page_cost}, что может быть неоптимально для SSD",
                recommendation="Для SSD рассуйте установку random_page_cost в 1.0-1.1 для более точного планирования"
            ))

        # Если random_page_cost слишком большой
        elif random_page_cost > 1.5:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"random_page_cost установлен в {random_page_cost}, что может быть завышен для современных дисков",
                recommendation="Рассуйте уменьшение random_page_cost до 1.1-1.3 для улучшения производительности"
            ))

    except Exception as e:
        print(f"Error analyzing random_page_cost settings: {e}")

    return recommendations


def analyze_seq_page_cost_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек seq_page_cost
    """
    recommendations = []

    try:
        seq_page_cost = float(db_settings.get('seq_page_cost', {}).get('setting', '1.0'))

        # Если seq_page_cost не равен 1.0 (стандартное значение)
        if seq_page_cost != 1.0:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"seq_page_cost установлен в {seq_page_cost}, что может быть неоптимально",
                recommendation="Рассуйте установку seq_page_cost в 1.0 (стандартное значение) для корректного планирования"
            ))

    except Exception as e:
        print(f"Error analyzing seq_page_cost settings: {e}")

    return recommendations


def analyze_effective_io_concurrency_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек effective_io_concurrency
    """
    recommendations = []

    try:
        effective_io_concurrency = int(db_settings.get('effective_io_concurrency', {}).get('setting', '1'))

        # Если effective_io_concurrency слишком маленький
        if effective_io_concurrency < 2:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"effective_io_concurrency установлен в {effective_io_concurrency}, что может быть неоптимально для SSD",
                recommendation="Для SSD рассуйте увеличение effective_io_concurrency до 100-200 для улучшения параллельного чтения"
            ))

        # Если effective_io_concurrency слишком большой
        elif effective_io_concurrency > 200:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"effective_io_concurrency установлен в {effective_io_concurrency}, что может вызывать проблемы",
                recommendation="Рассуйте уменьшение effective_io_concurrency до 100-200 для предотвращения чрезмерной нагрузки на диск"
            ))

    except Exception as e:
        print(f"Error analyzing effective_io_concurrency settings: {e}")

    return recommendations


def analyze_checkpoint_segments_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек checkpoint_segments
    """
    recommendations = []

    try:
        checkpoint_segments = int(db_settings.get('checkpoint_segments', {}).get('setting', '3'))

        # Если checkpoint_segments слишком маленький
        if checkpoint_segments < 3:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"checkpoint_segments установлен в {checkpoint_segments}, что может вызывать частые чекпоинты",
                recommendation="Рассуйте увеличение checkpoint_segments до 3-5 для уменьшения частоты чекпоинтов"
            ))

        # Если checkpoint_segments слишком большой
        elif checkpoint_segments > 10:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"checkpoint_segments установлен в {checkpoint_segments}, что может вызывать долгое восстановление",
                recommendation="Рассуйте уменьшение checkpoint_segments до 3-5 для ускорения восстановления после сбоя"
            ))

    except Exception as e:
        print(f"Error analyzing checkpoint_segments settings: {e}")

    return recommendations


def analyze_checkpoint_completion_target_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек checkpoint_completion_target
    """
    recommendations = []

    try:
        checkpoint_completion_target = float(db_settings.get('checkpoint_completion_target', {}).get('setting', '0.5'))

        # Если checkpoint_completion_target не оптимальный
        if checkpoint_completion_target < 0.7:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"checkpoint_completion_target установлен в {checkpoint_completion_target}, что может вызывать пиковую нагрузку",
                recommendation="Рассуйте увеличение checkpoint_completion_target до 0.9 для более равномерной нагрузки"
            ))

        elif checkpoint_completion_target > 0.9:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"checkpoint_completion_target установлен в {checkpoint_completion_target}, что может замедлять чекпоинты",
                recommendation="Рассуйте уменьшение checkpoint_completion_target до 0.7-0.9 для ускорения чекпоинтов"
            ))

    except Exception as e:
        print(f"Error analyzing checkpoint_completion_target settings: {e}")

    return recommendations


def analyze_wal_buffers_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек wal_buffers
    """
    recommendations = []

    try:
        wal_buffers = db_settings.get('wal_buffers', {}).get('setting', '16MB')

        # Парсинг значения wal_buffers
        wal_buffers_mb = parse_memory_value(wal_buffers)

        # Если wal_buffers слишком маленький
        if wal_buffers_mb < 16:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"wal_buffers установлен в {wal_buffers}, что может быть недостаточно для высокой нагрузки",
                recommendation="Рассуйте увеличение wal_buffers до 16-64MB для улучшения производительности записи"
            ))

        # Если wal_buffers слишком большой
        elif wal_buffers_mb > 64:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"wal_buffers установлен в {wal_buffers}, что может потреблять слишком много памяти",
                recommendation="Рассуйте уменьшение wal_buffers до 16-32MB для предотвращения нехватки памяти"
            ))

    except Exception as e:
        print(f"Error analyzing wal_buffers settings: {e}")

    return recommendations


def analyze_max_connections_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек max_connections
    """
    recommendations = []

    try:
        max_connections = int(db_settings.get('max_connections', {}).get('setting', '100'))

        # Если max_connections слишком большой
        if max_connections > 200:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"max_connections установлен в {max_connections}, что может потреблять слишком много памяти",
                recommendation="Рассуйте уменьшение max_connections до 100-200 для предотвращения нехватки памяти"
            ))

        # Если max_connections слишком маленький
        elif max_connections < 50:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"max_connections установлен в {max_connections}, что может быть недостаточно для высокой нагрузки",
                recommendation="Рассуйте увеличение max_connections до 100-200 для обработки большего количества одновременных подключений"
            ))

    except Exception as e:
        print(f"Error analyzing max_connections settings: {e}")

    return recommendations


def analyze_ssl_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек SSL
    """
    recommendations = []

    try:
        ssl = db_settings.get('ssl', {}).get('setting', 'off')

        # Если SSL выключен, но это производственная среда
        if ssl.lower() == 'off':
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="HIGH",
                message="SSL выключен, что может быть небезопасно для производственной среды",
                recommendation="Рассуйте включение SSL для шифрования данных в транзите"
            ))

    except Exception as e:
        print(f"Error analyzing SSL settings: {e}")

    return recommendations


def parse_memory_value(value_str: str) -> int:
    """
    Парсинг строкового значения памяти в мегабайты
    """
    try:
        value_str = value_str.strip().upper()

        if value_str.endswith('KB'):
            return int(float(value_str[:-2]) / 1024)
        elif value_str.endswith('MB'):
            return int(float(value_str[:-2]))
        elif value_str.endswith('GB'):
            return int(float(value_str[:-2]) * 1024)
        elif value_str.endswith('TB'):
            return int(float(value_str[:-2]) * 1024 * 1024)
        else:
            # Предполагаем, что это уже в байтах
            return int(int(float(value_str)) / (1024 * 1024))
    except:
        return 0
