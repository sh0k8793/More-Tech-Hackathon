import re
from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_inefficient_order_by(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает неэффективные операции ORDER BY
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие ORDER BY в запросе
        if 'ORDER BY' in query.upper():
            # Проверяем наличие Sort в плане выполнения
            if 'plan' in plan:
                plan_content = str(plan['plan'])
                if 'Sort' in plan_content:
                    # Проверяем, есть ли индекс для сортировки
                    has_index_for_sort = False
                    if 'indexes' in context:
                        for index in context['indexes']:
                            if 'index_columns' in index and 'order_by_columns' in context:
                                # Простая проверка: если столбцы индекса совпадают со столбцами ORDER BY
                                if set(index['index_columns']) & set(context['order_by_columns']):
                                    has_index_for_sort = True
                                    break

                    if not has_index_for_sort:
                        # Проверяем количество строк для сортировки
                        rows_match = re.search(r'Sort.*rows=(\d+)', plan_content)  # noqa: F821
                        if rows_match and int(rows_match.group(1)) > 10000:
                            diagnose = LintDiagnose(
                                line=1,
                                col=1,
                                severity="MEDIUM",
                                message="Обнаружена неэффективная операция сортировки большого объема данных",
                                recommendation="Рассмотрите возможность добавления индекса по столбцам, используемым в ORDER BY, или ограничения количества возвращаемых строк с помощью LIMIT."
                            )
                            recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in inefficient_order_by rule: {e}")

    return recommendations, optimized_query #ty: ignore
