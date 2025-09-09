import re
from typing import Any, Dict, List, Set

from ....models.lint_diagnose import LintDiagnose


def rule_partitioning_analysis(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ возможностей секционирования таблиц
    """
    recommendations = []

    try:
        # Анализ статистики таблиц для выявления кандидатов на секционирование
        table_stats = context.get('table_stats', {})

        # Рекомендации по секционированию больших таблиц
        partitioning_recommendations = analyze_partitioning_candidates(table_stats, context)
        recommendations.extend(partitioning_recommendations)

        # Рекомендации по выбору стратегии секционирования
        partitioning_strategy_recommendations = analyze_partitioning_strategy(table_stats, context)
        recommendations.extend(partitioning_strategy_recommendations)

        # Рекомендации по выбору ключа секционирования
        partitioning_key_recommendations = analyze_partitioning_key(query, table_stats, context)
        recommendations.extend(partitioning_key_recommendations)

        # Рекомендации по управлению секциями
        partitioning_management_recommendations = analyze_partitioning_management(table_stats, context)
        recommendations.extend(partitioning_management_recommendations)

        # Рекомендации по производительности секционированных таблиц
        partitioning_performance_recommendations = analyze_partitioning_performance(query, plan, context)
        recommendations.extend(partitioning_performance_recommendations)

    except Exception as e:
        print(f"Error in partitioning_analysis rule: {e}")

    return recommendations


def analyze_partitioning_candidates(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ кандидатов на секционирование
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            row_count = stats.get('row_count', 0)
            dead_rows = stats.get('dead_rows', 0)

            # Парсинг размера таблицы
            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая
            if size_in_mb > 1000:  # Больше 1GB
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} ({table_size}) является кандидатом на секционирование",
                    recommendation=f"Рассуйте секционирование таблицы {table_name} для улучшения производительности запросов и управления данными"
                ))

            # Если таблица очень большая
            elif size_in_mb > 5000:  # Больше 5GB
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"Таблица {table_name} ({table_size}) очень большая и настоятельно нуждается в секционировании",
                    recommendation=f"Настоятельно рекомендую секционирование таблицы {table_name} для значительного улучшения производительности"
                ))

            # Если таблица имеет много строк и часто обновляется
            if row_count > 1000000 and dead_rows / row_count > 0.1:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Таблица {table_name} имеет много строк и высокую фрагментацию ({dead_rows / row_count:.1%} мертвых строк)",
                    recommendation=f"Рассуйте секционирование таблицы {table_name} для улучшения производительности операций обслуживания"
                ))

    except Exception as e:
        print(f"Error analyzing partitioning candidates: {e}")

    return recommendations


def analyze_partitioning_strategy(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ выбора стратегии секционирования
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            size_in_mb = parse_memory_value(table_size)

            # Если таблица очень большая, рекомендовать RANGE секционирование
            if size_in_mb > 10000:  # Больше 10GB
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Для очень большой таблицы {table_name} рекомендуется RANGE секционирование",
                    recommendation=f"Рассуйте использование RANGE секционирования по дате или ID для таблицы {table_name}"
                ))

            # Если таблица имеет много строк с разными значениями, рекомендовать LIST секционирование
            elif size_in_mb > 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Для большой таблицы {table_name}可以考虑 LIST секционирование",
                    recommendation=f"Рассуйте использование LIST секционирования по категориальным значениям для таблицы {table_name}"
                ))

            # Если таблица имеет данные с естественной хронологией, рекомендовать HASH секционирование
            elif size_in_mb > 500:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Для средней таблицы {table_name} можно рассмотреть HASH секционирование",
                    recommendation=f"Рассуйте использование HASH секционирования для равномерного распределения данных в таблице {table_name}"
                ))

    except Exception as e:
        print(f"Error analyzing partitioning strategy: {e}")

    return recommendations


def analyze_partitioning_key(query: str, table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ выбора ключа секционирования
    """
    recommendations = []

    try:
        # Извлечение таблиц из запроса
        tables_in_query = extract_tables_from_query(query)

        for table_name in tables_in_query:
            if table_name in table_stats:
                stats = table_stats[table_name]
                table_size = stats.get('table_size', '0')
                size_in_mb = parse_memory_value(table_size)

                # Если таблица большая, анализируем запросы для выбора ключа
                if size_in_mb > 1000:
                    # Проверяем, есть ли в запросе условия по дате
                    if re.search(r'where\s+\w+\s*(>=|<=|=|>|<)\s*["\']?\d{4}-\d{2}-\d{2}["\']?', query, re.IGNORECASE):
                        recommendations.append(LintDiagnose(
                            line=1,
                            col=1,
                            severity="MEDIUM",
                            message=f"Для таблицы {table_name} рекомендуется RANGE секционирование по дате",
                            recommendation=f"Рассуйте RANGE секционирование по дате для таблицы {table_name} для ускорения запросов с фильтрацией по дате"
                        ))

                    # Проверяем, есть ли в запросе условия по ID
                    elif re.search(r'where\s+\w+\s*(>=|<=|=|>|<)\s*\d+', query, re.IGNORECASE):
                        recommendations.append(LintDiagnose(
                            line=1,
                            col=1,
                            severity="MEDIUM",
                            message=f"Для таблицы {table_name} рекомендуется RANGE секционирование по ID",
                            recommendation=f"Рассуйте RANGE секционирование по ID для таблицы {table_name} для ускорения запросов с фильтрацией по ID"
                        ))

                    # Проверяем, есть ли в запросе условия по категории
                    elif re.search(r'where\s+\w+\s*in\s*\([^)]+\)', query, re.IGNORECASE):
                        recommendations.append(LintDiagnose(
                            line=1,
                            col=1,
                            severity="LOW",
                            message=f"Для таблицы {table_name} можно рассмотреть LIST секционирование",
                            recommendation=f"Рассуйте LIST секционирование по категориальным значениям для таблицы {table_name}"
                        ))

                    # Если нет явных условий, рекомендовать HASH секционирование
                    else:
                        recommendations.append(LintDiagnose(
                            line=1,
                            col=1,
                            severity="LOW",
                            message=f"Для таблицы {table_name} рекомендуется HASH секционирование",
                            recommendation=f"Рассуйте HASH секционирование для равномерного распределения данных в таблице {table_name}"
                        ))

    except Exception as e:
        print(f"Error analyzing partitioning key: {e}")

    return recommendations


def analyze_partitioning_management(table_stats: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ управления секциями
    """
    recommendations = []

    try:
        for table_name, stats in table_stats.items():
            table_size = stats.get('table_size', '0')
            size_in_mb = parse_memory_value(table_size)

            # Если таблица большая, рекомендовать автоматическое управление секциями
            if size_in_mb > 1000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Для секционированной таблицы {table_name} рекомендуется автоматическое управление секциями",
                    recommendation=f"Рассуйте создание процедуры автоматического добавления новых секций для таблицы {table_name}"
                ))

            # Если таблица очень большая, рекомендовать политику старения данных
            elif size_in_mb > 5000:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Для очень большой таблицы {table_name} рекомендуется политика старения данных",
                    recommendation=f"Рассуйте автоматическое удаление старых секций для таблицы {table_name} для управления размером базы данных"
                ))

    except Exception as e:
        print(f"Error analyzing partitioning management: {e}")

    return recommendations


def analyze_partitioning_performance(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ производительности секционированных таблиц
    """
    recommendations = []

    try:
        # Анализ плана выполнения на предмет секционирования
        if plan and isinstance(plan, list):
            for node in plan:
                if isinstance(node, dict):
                    node_type = node.get('Node Type', '')

                    # Если есть последовательное сканирование большой таблицы
                    if node_type == 'Seq Scan':
                        relation_name = node.get('Relation Name', '')
                        if relation_name:
                            table_stats = context.get('table_stats', {})
                            if relation_name in table_stats:
                                table_size = table_stats[relation_name].get('table_size', '0')
                                size_in_mb = parse_memory_value(table_size)

                                if size_in_mb > 1000:
                                    recommendations.append(LintDiagnose(
                                        line=1,
                                        col=1,
                                        severity="MEDIUM",
                                        message=f"Большая таблица {relation_name} сканируется последовательно, что можно оптимизировать секционированием",
                                        recommendation=f"Рассуйте секционирование таблицы {relation_name} для ускорения запросов с фильтрацией"
                                    ))

                    # Если есть сканирование по индексу без секционирования
                    elif node_type == 'Index Scan':
                        relation_name = node.get('Relation Name', '')
                        if relation_name:
                            table_stats = context.get('table_stats', {})
                            if relation_name in table_stats:
                                table_size = table_stats[relation_name].get('table_size', '0')
                                size_in_mb = parse_memory_value(table_size)

                                if size_in_mb > 1000:
                                    recommendations.append(LintDiagnose(
                                        line=1,
                                        col=1,
                                        severity="LOW",
                                        message=f"Для большой таблицы {relation_name} можно рассмотреть секционирование для улучшения производительности индексов",
                                        recommendation=f"Рассуйте секционирование таблицы {relation_name} для улучшения производительности индексных операций"
                                    ))

    except Exception as e:
        print(f"Error analyzing partitioning performance: {e}")

    return recommendations


def extract_tables_from_query(query: str) -> Set[str]:
    """
    Извлечение таблиц из SQL запроса
    """
    tables = set()

    try:
        # Простая реализация - в реальном приложении нужен более сложный парсер
        from_pattern = r'from\s+([^\s,]+)'
        join_pattern = r'join\s+([^\s,]+)'

        from_matches = re.findall(from_pattern, query, re.IGNORECASE)
        join_matches = re.findall(join_pattern, query, re.IGNORECASE)

        tables.update(from_matches)
        tables.update(join_matches)

        # Удаление алиасов и схемы
        clean_tables = set()
        for table in tables:
            table = table.strip()
            if '.' in table:
                table = table.split('.')[-1]  # Берем только имя таблицы
            if table.upper() not in ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'JOIN', 'ON', 'AND', 'OR', 'HAVING', 'UNION']:
                clean_tables.add(table)

        return clean_tables

    except Exception as e:
        print(f"Error extracting tables from query: {e}")
        return set()


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
