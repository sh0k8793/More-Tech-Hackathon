import json
from typing import Dict, List, Any, Optional
from ....models.lint_diagnose import LintDiagnose


def rule_cost_estimation(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализирует план выполнения для оценки стоимости запроса
    """
    recommendations = []

    try:
        if not plan or not isinstance(plan, list):
            return recommendations

        # Анализ корневого узла плана
        root_node = {}
        if plan and isinstance(plan, list) and len(plan) > 0:
            # Use list unpacking to avoid type issues
            if len(plan) > 0:
                first_element = plan[0]
                if isinstance(first_element, dict):
                    root_node = first_element

        # Извлечение метрик из плана
        total_cost = root_node.get('Total Cost', 0)
        planning_time = root_node.get('Planning Time', 0)
        execution_time = root_node.get('Execution Time', 0)

        # Анализ узлов выполнения
        nodes = analyze_plan_nodes(root_node)

        # Оценка ожидаемого времени выполнения
        if total_cost > 1000:  # Порог для дорогостоящих запросов
            severity = "HIGH" if total_cost > 10000 else "MEDIUM"

            diagnose = LintDiagnose(
                line=1,
                col=1,
                severity=severity,
                message=f"Запрос имеет высокую ожидаемую стоимость: {total_cost:.2f}",
                recommendation=f"Рассмотрите оптимизацию запроса. Ожидаемое время выполнения: {planning_time + execution_time:.2f} мс"
            )
            recommendations.append(diagnose)

        # Анализ сканирования таблиц
        scan_recommendations = analyze_table_scans(nodes, context)
        recommendations.extend(scan_recommendations)

        # Анализ использования индексов
        index_recommendations = analyze_index_usage(nodes, context)
        recommendations.extend(index_recommendations)

        # Анализ блокировок
        lock_recommendations = analyze_locking_behavior(nodes)
        recommendations.extend(lock_recommendations)

    except Exception as e:
        print(f"Error in cost_estimation rule: {e}")

    return recommendations


def analyze_plan_nodes(node: Dict[str, Any], nodes_list: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Рекурсивный анализ узлов плана выполнения
    """
    if nodes_list is None:
        nodes_list = []

    nodes_list.append(node)

    # Рекурсивный анализ дочерних узлов
    for child in node.get('Plans', []):
        analyze_plan_nodes(child, nodes_list)

    return nodes_list


def analyze_table_scans(nodes: List[Dict[str, Any]], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ сканирования таблиц на предмет неэффективности
    """
    recommendations = []

    for node in nodes:
        node_type = node.get('Node Type', '')

        # Поиск последовательного сканирования больших таблиц
        if node_type == 'Seq Scan':
            relname = node.get('Relation Name', '')
            if relname:
                # Проверяем размер таблицы из контекста
                table_stats = context.get('table_stats', {})
                table_key = relname if 'public.' + relname not in table_stats else 'public.' + relname

                if table_key in table_stats:
                    table_info = table_stats[table_key]
                    table_size = table_info.get('table_size', '0')

                    # Если таблица большая и используется последовательное сканирование
                    if 'MB' in table_size or 'GB' in table_size:
                        severity = "HIGH" if 'GB' in table_size else "MEDIUM"

                        diagnose = LintDiagnose(
                            line=1,
                            col=1,
                            severity=severity,
                            message=f"Обнаружено последовательное сканирование таблицы {relname} размером {table_size}",
                            recommendation=f"Рассмотрите создание индекса для таблицы {relname} или использование условной выборки для уменьшения объема сканирования"
                        )
                        recommendations.append(diagnose)

        # Анализ сканирования по индексу без извлечения строк
        elif node_type == 'Index Scan':
            if node.get('Index Cond', '') and node.get('Heap Fetches', 0) > 0:
                heap_fetches = node.get('Heap Fetches', 0)
                rows = node.get('Rows', 0)

                # Если соотношение извлеченных строк к сканируемым высокое
                if rows > 0 and heap_fetches / rows > 0.8:
                    diagnose = LintDiagnose(
                        line=1,
                        col=1,
                        severity="MEDIUM",
                        message="Высокое соотношение извлечения строк из кучи при индексном сканировании",
                        recommendation="Рассмотрите включение часто используемых столбцов в составной индекс для уменьшения количества извлечений из кучи"
                    )
                    recommendations.append(diagnose)

    return recommendations


def analyze_index_usage(nodes: List[Dict[str, Any]], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ использования индексов
    """
    recommendations = []

    for node in nodes:
        node_type = node.get('Node Type', '')

        # Поиск сканирования без индекса
        if node_type in ['Seq Scan', 'Bitmap Heap Scan'] and not node.get('Index Name'):
            relname = node.get('Relation Name', '')
            if relname:
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message=f"Для таблицы {relname} не используется индекс",
                    recommendation=f"Рассмотрите создание индекса для часто используемых условий WHERE и JOIN в запросах к таблице {relname}"
                )
                recommendations.append(diagnose)

        # Анализ неэффективных индексов
        elif node_type == 'Index Scan':
            index_name = node.get('Index Name', '')
            if index_name:
                # Проверяем статистику использования индекса
                index_stats = context.get('index_stats', [])
                for idx_stat in index_stats:
                    if idx_stat.get('index') == index_name:
                        scans = idx_stat.get('scans', 0)
                        if scans == 0:
                            diagnose = LintDiagnose(
                                line=1,
                                col=1,
                                severity="LOW",
                                message=f"Индекс {index_name} не используется",
                                recommendation=f"Рассмотрите удаление неиспользуемого индекса {index_name} для улучшения производительности операций вставки, обновления и удаления"
                            )
                            recommendations.append(diagnose)
                        break

    return recommendations


def analyze_locking_behavior(nodes: List[Dict[str, Any]]) -> List[LintDiagnose]:
    """
    Анализ поведения блокировок
    """
    recommendations = []

    # Поиск узлов с блокировками
    for node in nodes:
        node_type = node.get('Node Type', '')

        # Поиск операций, которые могут вызывать блокировки
        if node_type in ['Lock Rows', 'Update', 'Delete', 'Insert']:
            severity = "HIGH" if node_type in ['Update', 'Delete', 'Insert'] else "MEDIUM"

            diagnose = LintDiagnose(
                line=1,
                col=1,
                severity=severity,
                message=f"Обнаружена операция {node_type}, которая может вызывать блокировки",
                recommendation="Рассмотрите использование транзакций с уровнем изоляции READ COMMITTED или оптимизацию запроса для уменьшения времени удержания блокировок"
            )
            recommendations.append(diagnose)

    return recommendations
