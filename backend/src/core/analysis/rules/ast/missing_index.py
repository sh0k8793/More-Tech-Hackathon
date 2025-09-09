import re
from typing import Any, Dict, List

from core.models.lint_diagnose import LintDiagnose


def rule_missing_index(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает запросы, которые могут выиграть от добавления индексов
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие Seq Scan в плане выполнения
        if 'plan' in plan:
            plan_content = str(plan['plan'])
            if 'Seq Scan' in plan_content and 'Filter' in plan_content:
                # Извлекаем имя таблицы из плана
                table_match = re.search(r'Seq Scan on (\w+)', plan_content)
                if table_match:
                    table_name = table_match.group(1)

                    # Извлекаем условия фильтрации
                    filter_match = re.search(r'Filter: \(([^)]+)\)', plan_content)
                    if filter_match:
                        filter_condition = filter_match.group(1)

                        # Проверяем, есть ли уже индекс для этой таблицы
                        has_index = False
                        if 'indexes' in context:
                            for index in context['indexes']:
                                if index['table'] == table_name:
                                    has_index = True
                                    break

                        if not has_index:
                            diagnose = LintDiagnose(
                                line=1,
                                col=1,
                                severity="HIGH",
                                message=f"Отсутствует индекс для таблицы {table_name} с условием фильтрации",
                                recommendation=f"Рассмотрите возможность добавления индекса для таблицы {table_name} по столбцам, используемым в условии фильтрации: {filter_condition}. Это может значительно ускорить выполнение запроса."
                            )
                            recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in missing_index rule: {e}")

    return recommendations, optimized_query #ty: ignore
