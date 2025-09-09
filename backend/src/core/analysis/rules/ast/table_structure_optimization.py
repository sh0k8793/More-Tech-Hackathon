from typing import Any, Dict, List

from ....models.lint_diagnose import LintDiagnose


def rule_table_structure_optimization(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Рекомендации по оптимизации структуры таблиц
    """
    recommendations = []

    try:
        # Анализ статистики таблиц
        table_stats = context.get('table_stats', {})

        # Рекомендации по выбору типов данных
        data_type_recommendations = analyze_data_types(table_stats, context)
        recommendations.extend(data_type_recommendations)

        # Рекомендации по нормализации денормализации
        normalization_recommendations = analyze_normalization(table_stats, context)
        recommendations.extend(normalization_recommendations)

        # Рекомендации по внешним ключам
        foreign_key_recommendations = analyze_foreign_keys(table_stats, context)
        recommendations.extend(foreign_key_recommendations)

        # Рекомендации по ограничениям
        constraint_recommendations = analyze_constraints(table_stats, context)
        recommendations.extend(constraint_recommendations)

        # Рекомендации по индексам
        index_recommendations = analyze_table_indexes(table_stats, context)
        recommendations.extend(index_recommendations)

        # Рекомендации по кластеризации
        clustering_recommendations = analyze_clustering(table_stats, context)
        recommendations.extend(clustering_recommendations)

        # Рекомендации по наследованию
        inheritance_recommendations = analyze_inheritance(table_stats, context)
        recommendations.extend(inheritance_recommendations)

        # Рекомендации по TOAST
        toast_recommendations = analyze_toast(table_stats, context)
        recommendations.extend(toast_recommendations)

    except Exception as e:
        print(f"Error in table_structure_optimization rule: {e}")

    return recommendations


def analyze_data_types(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ выбора типов данных
    """
    recommendations = []

    try:
        # В реальном приложении здесь нужно получить информацию о типах данных столбцов
        # Пока используем эвристики на основе статистики таблиц

        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)
            dead_rows = stats.get('dead_rows', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, проверяем возможность оптимизации типов данных
            if size_in_mb > 100:
                # Если таблица имеет много строк, возможно использование более компактных типов
                if row_count > 1000000:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="LOW",
                        message=f"Таблица {table_name} имеет много строк ({row_count:,}), можно рассмотреть оптимизацию типов данных",
                        recommendation=f"Рассуйте использование более компактных типов данных для таблицы {table_name} для уменьшения размера"
                    ))

                # Если таблица имеет много мертвых строк, возможно использование более эффективных типов
                if dead_rows / row_count > 0.1:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="LOW",
                        message=f"Таблица {table_name} имеет высокую фрагментацию ({dead_rows / row_count:.1%}), можно рассмотреть оптимизацию типов данных",
                        recommendation=f"Рассуйте использование более эффективных типов данных для таблицы {table_name} для уменьшения фрагментации"
                    ))

                # Если таблица очень большая, возможно использование сжатых типов
                if size_in_mb > 1000:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="MEDIUM",
                        message=f"Таблица {table_name} очень большая ({table_size}), можно рассмотреть использование сжатых типов данных",
                        recommendation=f"Рассуйте использование сжатых типов данных (например, SMALLINT вместо INTEGER) для таблицы {table_name}"
                    ))

    except Exception as e:
        print(f"Error analyzing data types: {e}")

    return recommendations


def analyze_normalization(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ нормализации и денормализации
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица очень большая, возможно денормализация
            if size_in_mb > 5000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} очень большая ({table_size}), можно рассмотреть денормализацию",
                    recommendation=f"Рассуйте денормализацию таблицы {table_name} для улучшения производительности чтения"
                ))

            # Если таблица имеет много строк и часто используется в JOIN, возможно денормализация
            elif row_count > 1000000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} имеет много строк ({row_count:,}), можно рассмотреть денормализацию",
                    recommendation=f"Рассуйте денормализацию таблицы {table_name} для уменьшения количества JOIN операций"
                ))

            # Если таблица маленькая, возможно нормализация
            elif size_in_mb < 10 and row_count < 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} маленькая ({table_size}), можно рассмотреть нормализацию",
                    recommendation=f"Рассуйте нормализацию таблицы {table_name} для уменьшения дублирования данных"
                ))

    except Exception as e:
        print(f"Error analyzing normalization: {e}")

    return recommendations


def analyze_foreign_keys(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ внешних ключей
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, возможно нужно индексы для внешних ключей
            if size_in_mb > 100:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} большая ({table_size}), возможно нужны индексы для внешних ключей",
                    recommendation=f"Рассуйте создание индексов для внешних ключей в таблице {table_name} для улучшения производительности JOIN"
                ))

            # Если таблица очень большая, возможно нужно отложенная проверка внешних ключей
            if size_in_mb > 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} очень большая ({table_size}), можно рассмотреть отложенную проверку внешних ключей",
                    recommendation=f"Рассуйте использование DEFERRABLE внешних ключей для таблицы {table_name} для улучшения производительности массовых операций"
                ))

    except Exception as e:
        print(f"Error analyzing foreign keys: {e}")

    return recommendations


def analyze_constraints(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ ограничений
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, возможно нужно отложенные проверки ограничений
            if size_in_mb > 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} большая ({table_size}), можно рассмотреть отложенные проверки ограничений",
                    recommendation=f"Рассуйте использование DEFERRABLE ограничений для таблицы {table_name} для улучшения производительности массовых операций"
                ))

            # Если таблица имеет много строк, возможно нужно частичные индексы
            if row_count > 1000000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} имеет много строк ({row_count:,}), можно рассмотреть частичные индексы",
                    recommendation=f"Рассуйте создание частичных индексов для таблицы {table_name} для улучшения производительности запросов"
                ))

    except Exception as e:
        print(f"Error analyzing constraints: {e}")

    return recommendations


def analyze_table_indexes(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ индексов таблиц
    """
    recommendations = []

    try:
        index_stats = context.get('index_stats', [])

        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, возможно нужно больше индексов
            if size_in_mb > 100:
                # Проверяем количество индексов для таблицы
                table_indexes = [idx for idx in index_stats if idx.get('table') == table_name]

                # Если мало индексов
                if len(table_indexes) < 2:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="MEDIUM",
                        message=f"Таблица {table_name} имеет мало индексов ({len(table_indexes)}), можно рассмотреть создание дополнительных индексов",
                        recommendation=f"Рассуйте создание индексов для часто используемых столбцов в таблице {table_name}"
                    ))

                # Если много индексов
                elif len(table_indexes) > 10:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="LOW",
                        message=f"Таблица {table_name} имеет много индексов ({len(table_indexes)}), можно рассовать удаление неиспользуемых",
                        recommendation=f"Рассуйте удаление неиспользуемых индексов из таблицы {table_name} для улучшения производительности операций записи"
                    ))

    except Exception as e:
        print(f"Error analyzing table indexes: {e}")

    return recommendations


def analyze_clustering(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ кластеризации
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, возможно кластеризация
            if size_in_mb > 500:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} большая ({table_size}), можно рассмотреть кластеризацию",
                    recommendation=f"Рассуйте кластеризацию таблицы {table_name} по часто используемым столбцам для улучшения производительности чтения"
                ))

            # Если таблица очень большая, возможно частичная кластеризация
            if size_in_mb > 2000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} очень большая ({table_size}), можно рассовать частичную кластеризацию",
                    recommendation=f"Рассуйте частичную кластеризацию таблицы {table_name} для улучшения производительности запросов"
                ))

    except Exception as e:
        print(f"Error analyzing clustering: {e}")

    return recommendations


def analyze_inheritance(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ наследования
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, возможно наследование
            if size_in_mb > 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} большая ({table_size}), можно рассовать наследование",
                    recommendation=f"Рассуйте использование наследования для таблицы {table_name} для улучшения организации данных"
                ))

            # Если таблица имеет много строк, возможно разделение на дочерние таблицы
            if row_count > 5000000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"Таблица {table_name} имеет очень много строк ({row_count:,}), можно рассовать разделение на дочерние таблицы",
                    recommendation=f"Рассуйте разделение таблицы {table_name} на дочерние таблицы для улучшения производительности"
                ))

    except Exception as e:
        print(f"Error analyzing inheritance: {e}")

    return recommendations


def analyze_toast(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ TOAST
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            stats.get('row_count', 0)

            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, возможно настройка TOAST
            if size_in_mb > 100:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Таблица {table_name} большая ({table_size}), можно рассовать настройку TOAST",
                    recommendation=f"Рассуйте настройку параметров TOAST для таблицы {table_name} для оптимизации хранения больших данных"
                ))

            # Если таблица очень большая, возможно сжатие TOAST
            if size_in_mb > 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} очень большая ({table_size}), можно рассовать сжатие TOAST",
                    recommendation=f"Рассуйте включение сжатия TOAST для таблицы {table_name} для уменьшения размера базы данных"
                ))

    except Exception as e:
        print(f"Error analyzing TOAST: {e}")

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
    except Exception:
        return 0
