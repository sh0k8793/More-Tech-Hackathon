from typing import Any, Dict, List, Set

import sqlparse

from ....models.lint_diagnose import LintDiagnose


def rule_index_optimization(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализирует запросы на предмет возможностей оптимизации индексами
    """
    recommendations = []

    try:
        # Парсинг запроса для выявления таблиц и условий
        parsed_query = parse_query_for_tables_and_conditions(query)

        # Анализ текущих индексов
        current_indexes = extract_current_indexes(context)

        # Поиск таблиц без индексов
        tables_without_indexes = find_tables_without_indexes(parsed_query['tables'], current_indexes)
        for table in tables_without_indexes:
            diagnose = LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message=f"Таблица {table} не имеет индексов",
                recommendation=f"Рассмотрите создание индекса для таблицы {table}, особенно для часто используемых условий WHERE и JOIN"
            )
            recommendations.append(diagnose)

        # Анализ условий WHERE для возможных индексов
        where_conditions = parsed_query.get('where_conditions', [])
        for condition in where_conditions:
            column, table, operator = condition
            potential_index_suggestions = suggest_indexes_for_condition(table, column, operator, current_indexes)
            for suggestion in potential_index_suggestions:
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity=suggestion['severity'],
                    message=suggestion['message'],
                    recommendation=suggestion['recommendation']
                )
                recommendations.append(diagnose)

        # Анализ JOIN условий
        join_conditions = parsed_query.get('join_conditions', [])
        for join in join_conditions:
            left_table, right_table, join_column = join
            join_index_suggestions = suggest_indexes_for_join(left_table, right_table, join_column, current_indexes)
            for suggestion in join_index_suggestions:
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity=suggestion['severity'],
                    message=suggestion['message'],
                    recommendation=suggestion['recommendation']
                )
                recommendations.append(diagnose)

        # Анализ ORDER BY для возможных индексов
        order_by_columns = parsed_query.get('order_by_columns', [])
        if order_by_columns:
            order_by_suggestions = suggest_indexes_for_order_by(order_by_columns, parsed_query['tables'], current_indexes)
            for suggestion in order_by_suggestions:
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity=suggestion['severity'],
                    message=suggestion['message'],
                    recommendation=suggestion['recommendation']
                )
                recommendations.append(diagnose)

        # Анализ GROUP BY для возможных индексов
        group_by_columns = parsed_query.get('group_by_columns', [])
        if group_by_columns:
            group_by_suggestions = suggest_indexes_for_group_by(group_by_columns, parsed_query['tables'], current_indexes)
            for suggestion in group_by_suggestions:
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity=suggestion['severity'],
                    message=suggestion['message'],
                    recommendation=suggestion['recommendation']
                )
                recommendations.append(diagnose)

        # Поиск неиспользуемых индексов
        unused_indexes = find_unused_indexes(current_indexes, parsed_query['tables'])
        for index in unused_indexes:
            diagnose = LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"Индекс {index['name']} не используется в запросах",
                recommendation=f"Рассмотрите удаление неиспользуемого индекса {index['name']} для улучшения производительности операций записи"
            )
            recommendations.append(diagnose)

    except Exception as e:
        print(f"Error in index_optimization rule: {e}")

    return recommendations


def parse_query_for_tables_and_conditions(query: str) -> Dict[str, Any]:
    """
    Парсинг SQL запроса для извлечения таблиц и условий
    """
    result = {
        'tables': set(),
        'where_conditions': [],
        'join_conditions': [],
        'order_by_columns': [],
        'group_by_columns': []
    }

    try:
        parsed = sqlparse.parse(query)

        for statement in parsed:
            # Извлечение таблиц из FROM и JOIN
            from_found = False
            for token in statement.tokens:
                if token.value.upper() == 'FROM':
                    from_found = True
                    continue
                elif from_found and token.value.upper() in ['WHERE', 'GROUP', 'ORDER', 'LIMIT', 'UNION', 'HAVING']:
                    from_found = False
                    break
                elif from_found and hasattr(token, 'tokens'):
                    # Извлечение имен таблиц
                    table_tokens = [str(t).strip() for t in token.tokens if str(t).strip() and not str(t).strip().startswith('(')]
                    for table_token in table_tokens:
                        if table_token and table_token.upper() not in ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'UNION', 'HAVING']:
                            result['tables'].add(table_token.split()[0])  # Берем только имя таблицы без алиасов

                # Анализ WHERE условий
                if token.value.upper() == 'WHERE':
                    # Простая реализация для извлечения условий
                    where_text = extract_where_conditions(statement)
                    result['where_conditions'].extend(where_text)

                # Анализ JOIN условий
                if 'JOIN' in token.value.upper():
                    join_conditions = extract_join_conditions(statement, token)
                    result['join_conditions'].extend(join_conditions)

                # Анализ ORDER BY
                if 'ORDER BY' in token.value.upper():
                    order_columns = extract_order_by_columns(statement, token)
                    result['order_by_columns'].extend(order_columns)

                # Анализ GROUP BY
                if 'GROUP BY' in token.value.upper():
                    group_columns = extract_group_by_columns(statement, token)
                    result['group_by_columns'].extend(group_columns)

    except Exception as e:
        print(f"Error parsing query: {e}")

    return result


def extract_where_conditions(statement) -> List[tuple]:
    """
    Извлечение условий WHERE из запроса
    """
    conditions = []
    # Простая реализация - в реальном приложении нужно использовать более сложный парсер
    return conditions


def extract_join_conditions(statement, join_token) -> List[tuple]:
    """
    Извлечение условий JOIN из запроса
    """
    conditions = []
    # Простая реализация
    return conditions


def extract_order_by_columns(statement, order_token) -> List[str]:
    """
    Извлечение столбцов ORDER BY
    """
    columns = []
    # Простая реализация
    return columns


def extract_group_by_columns(statement, group_token) -> List[str]:
    """
    Извлечение столбцов GROUP BY
    """
    columns = []
    # Простая реализация
    return columns


def extract_current_indexes(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Извлечение текущих индексов из контекста
    """
    indexes = []
    if 'index_stats' in context:
        for idx in context['index_stats']:
            indexes.append({
                'name': idx.get('index'),
                'table': idx.get('table'),
                'schema': idx.get('schema'),
                'scans': idx.get('scans', 0),
                'size': idx.get('size', '0')
            })
    return indexes


def find_tables_without_indexes(tables: Set[str], current_indexes: List[Dict[str, Any]]) -> List[str]:
    """
    Поиск таблиц без индексов
    """
    indexed_tables = set()
    for idx in current_indexes:
        indexed_tables.add(idx['table'])

    return [table for table in tables if table not in indexed_tables]


def suggest_indexes_for_condition(table: str, column: str, operator: str, current_indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Рекомендации по созданию индексов для условий WHERE
    """
    suggestions = []

    # Проверяем, существует ли уже индекс для этого столбца
    existing_index = any(idx['table'] == table and idx['name'] == f'idx_{table}_{column}' for idx in current_indexes)

    if not existing_index:
        severity = "HIGH" if operator.upper() in ['=', 'IN'] else "MEDIUM"

        suggestions.append({
            'severity': severity,
            'message': f"Для таблицы {table} столбец {column} в условии WHERE может проиндексироваться",
            'recommendation': f"Рассмотрите создание индекса CREATE INDEX idx_{table}_{column} ON {table}({column})"
        })

    return suggestions


def suggest_indexes_for_join(left_table: str, right_table: str, join_column: str, current_indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Рекомендации по созданию индексов для JOIN условий
    """
    suggestions = []

    # Проверяем индексы для правой таблицы в JOIN
    right_table_index_exists = any(
        idx['table'] == right_table and idx['name'] == f'idx_{right_table}_{join_column}'
        for idx in current_indexes
    )

    if not right_table_index_exists:
        suggestions.append({
            'severity': "HIGH",
            'message': f"Для JOIN операции между {left_table} и {right_table} не найден индекс на {join_column}",
            'recommendation': f"Рассмотрите создание индекса CREATE INDEX idx_{right_table}_{join_column} ON {right_table}({join_column})"
        })

    return suggestions


def suggest_indexes_for_order_by(order_columns: List[str], tables: Set[str], current_indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Рекомендации по созданию индексов для ORDER BY
    """
    suggestions = []

    if order_columns:
        # Проверяем наличие составного индекса для ORDER BY
        for table in tables:
            order_index_name = f'idx_{table}_order_{"_".join(order_columns)}'
            existing_order_index = any(idx['table'] == table and idx['name'] == order_index_name for idx in current_indexes)

            if not existing_order_index:
                suggestions.append({
                    'severity': "MEDIUM",
                    'message': f"Для ORDER BY {', '.join(order_columns)} может быть полезен составной индекс",
                    'recommendation': f"Рассмотрите создание индекса CREATE INDEX {order_index_name} ON {table}({', '.join(order_columns)})"
                })

    return suggestions


def suggest_indexes_for_group_by(group_columns: List[str], tables: Set[str], current_indexes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Рекомендации по созданию индексов для GROUP BY
    """
    suggestions = []

    if group_columns:
        # Проверяем наличие составного индекса для GROUP BY
        for table in tables:
            group_index_name = f'idx_{table}_group_{"_".join(group_columns)}'
            existing_group_index = any(idx['table'] == table and idx['name'] == group_index_name for idx in current_indexes)

            if not existing_group_index:
                suggestions.append({
                    'severity': "MEDIUM",
                    'message': f"Для GROUP BY {', '.join(group_columns)} может быть полезен составной индекс",
                    'recommendation': f"Рассмотрите создание индекса CREATE INDEX {group_index_name} ON {table}({', '.join(group_columns)})"
                })

    return suggestions


def find_unused_indexes(current_indexes: List[Dict[str, Any]], query_tables: Set[str]) -> List[Dict[str, Any]]:
    """
    Поиск неиспользуемых индексов
    """
    unused = []

    for idx in current_indexes:
        if idx['table'] in query_tables and idx['scans'] == 0:
            unused.append(idx)

    return unused
