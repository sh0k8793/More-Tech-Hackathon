import re
from typing import Dict, List, Any, Set
from ....models.lint_diagnose import LintDiagnose


def rule_n_plus_one_detection(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаружение N+1 проблем в запросах
    """
    recommendations = []

    try:
        # Анализ запросов на признаки N+1 проблем
        n_plus_one_recommendations = analyze_n_plus_one_patterns(query)
        recommendations.extend(n_plus_one_recommendations)

        # Анализ запросов на признаки подзапросов в циклах
        subquery_recommendations = analyze_subquery_in_loops(query)
        recommendations.extend(subquery_recommendations)

        # Анализ запросов на признаки множественных запросов к одной таблице
        multiple_query_recommendations = analyze_multiple_queries_to_same_table(query)
        recommendations.extend(multiple_query_recommendations)

        # Анализ запросов на признаки отсутствия JOIN
        missing_join_recommendations = analyze_missing_joins(query)
        recommendations.extend(missing_join_recommendations)

    except Exception as e:
        print(f"Error in n_plus_one_detection rule: {e}")

    return recommendations


def analyze_n_plus_one_patterns(query: str) -> List[LintDiagnose]:
    """
    Анализ запросов на классические N+1 паттерны
    """
    recommendations = []

    try:
        # Паттерн 1: SELECT в цикле с переменной
        # Ищем конструкции вида: for item in items: SELECT * FROM table WHERE id = item.id
        loop_patterns = [
            r'for\s+\w+\s+in\s+\w+:\s*select',
            r'foreach\s+\w+\s+as\s+\w+\s*:\s*select',
            r'while\s+\w+\s*:\s*select'
        ]

        for pattern in loop_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE | re.DOTALL)
            if matches:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message="Обнаружен потенциальный N+1 паттерн: SELECT в цикле",
                    recommendation="Рассуйте использование JOIN или IN для объединения данных в одном запросе вместо выполнения отдельных запросов в цикле"
                ))

        # Паттерн 2: Множественные SELECT запросы к одной таблице
        select_count = len(re.findall(r'\bselect\b', query, re.IGNORECASE))
        if select_count > 3:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"Обнаружено {select_count} SELECT запросов, что может указывать на N+1 проблему",
                recommendation="Рассуйте объединение нескольких SELECT запросов в один с помощью JOIN или UNION"
            ))

        # Паттерн 3: WHERE IN с большим количеством значений
        in_patterns = re.findall(r'where\s+\w+\s+in\s*\(([^)]+)\)', query, re.IGNORECASE | re.DOTALL)
        for in_clause in in_patterns:
            values = [v.strip() for v in in_clause.split(',') if v.strip()]
            if len(values) > 10:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"Обнаружен IN с большим количеством значений ({len(values)})",
                    recommendation="Рассуйте использование JOIN вместо IN с большим количеством значений для лучшей производительности"
                ))

    except Exception as e:
        print(f"Error analyzing N+1 patterns: {e}")

    return recommendations


def analyze_subquery_in_loops(query: str) -> List[LintDiagnose]:
    """
    Анализ подзапросов в циклах
    """
    recommendations = []

    try:
        # Ищем подзапросы внутри циклов
        loop_subquery_patterns = [
            r'for\s+\w+\s+in\s+\w+:\s*.*?select.*?from.*?where.*?\b\w+\.\w+\s*=',
            r'foreach\s+\w+\s+as\s+\w+\s*:\s*.*?select.*?from.*?where.*?\b\w+\.\w+\s*=',
            r'while\s+\w+\s*:\s*.*?select.*?from.*?where.*?\b\w+\.\w+\s*='
        ]

        for pattern in loop_subquery_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE | re.DOTALL)
            if matches:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message="Обнаружен подзапрос внутри цикла - классическая N+1 проблема",
                    recommendation="Рассуйте вынесение подзапроса за пределы цикла или использование JOIN для объединения данных"
                ))

        # Ищем вложенные подзапросы
        nested_subquery_pattern = r'select.*?from.*?where.*?in\s*\(\s*select'
        nested_matches = re.findall(nested_subquery_pattern, query, re.IGNORECASE | re.DOTALL)
        if nested_matches:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message="Обнаружены вложенные подзапросы, которые могут вызывать N+1 проблемы",
                recommendation="Рассуйте преобразование вложенных подзапросов в JOIN для улучшения производительности"
            ))

    except Exception as e:
        print(f"Error analyzing subqueries in loops: {e}")

    return recommendations


def analyze_multiple_queries_to_same_table(query: str) -> List[LintDiagnose]:
    """
    Анализ множественных запросов к одной таблице
    """
    recommendations = []

    try:
        # Извлекаем все таблицы из запроса
        tables = extract_tables_from_query(query)

        # Считаем количество запросов к каждой таблице
        table_query_count = {}
        for table in tables:
            table_pattern = r'\bselect\b.*?\bfrom\b\s+' + re.escape(table) + r'\b'
            matches = re.findall(table_pattern, query, re.IGNORECASE | re.DOTALL)
            table_query_count[table] = len(matches)

        # Если к одной таблице много запросов
        for table, count in table_query_count.items():
            if count > 2:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Обнаружено {count} запросов к таблице {table}",
                    recommendation=f"Рассуйте объединение запросов к таблице {table} в один с помощью JOIN для уменьшения количества round-trip"
                ))

    except Exception as e:
        print(f"Error analyzing multiple queries to same table: {e}")

    return recommendations


def analyze_missing_joins(query: str) -> List[LintDiagnose]:
    """
    Анализ отсутствующих JOIN
    """
    recommendations = []

    try:
        # Извлекаем все таблицы из запроса
        tables = extract_tables_from_query(query)

        # Если в запросе больше одной таблицы, но нет JOIN
        if len(tables) > 1:
            # Проверяем наличие JOIN
            join_keywords = ['join', 'inner join', 'left join', 'right join', 'full join', 'cross join']
            has_join = any(keyword in query.lower() for keyword in join_keywords)

            if not has_join:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message=f"В запросе несколько таблиц ({len(tables)}), но отсутствуют JOIN",
                    recommendation="Рассуйте добавление JOIN для объединения данных из нескольких таблиц вместо выполнения отдельных запросов"
                ))

        # Анализ WHERE условий на возможность JOIN
        where_conditions = extract_where_conditions(query)
        potential_joins = []

        for condition in where_conditions:
            # Ищем условия вида table1.id = table2.id
            if '.' in condition.get('value', ''):
                potential_joins.append(condition)

        if potential_joins:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"Обнаружены условия, которые могут быть преобразованы в JOIN ({len(potential_joins)})",
                recommendation="Рассуйте преобразование условий сравнения таблиц в явные JOIN для улучшения читаемости и производительности"
            ))

    except Exception as e:
        print(f"Error analyzing missing joins: {e}")

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


def extract_where_conditions(query: str) -> List[Dict[str, Any]]:
    """
    Извлечение условий WHERE из запроса
    """
    conditions = []

    try:
        # Поиск WHERE условий
        where_pattern = r'where\s+(.*?)(?:\s+group\s+by|\s+order\s+by|\s+limit|\s+union|$)'
        where_match = re.search(where_pattern, query, re.IGNORECASE | re.DOTALL)

        if where_match:
            where_clause = where_match.group(1)

            # Извлечение отдельных условий
            condition_pattern = r'(\w+(?:\.\w+)?)\s*(=|>|<|>=|<=|<>|in|like)\s*([^,\s]+)'
            condition_matches = re.findall(condition_pattern, where_clause, re.IGNORECASE)

            for column, operator, value in condition_matches:
                conditions.append({
                    'column': column,
                    'operator': operator,
                    'value': value,
                    'table': column.split('.')[0] if '.' in column else ''
                })

    except Exception as e:
        print(f"Error extracting where conditions: {e}")

    return conditions
