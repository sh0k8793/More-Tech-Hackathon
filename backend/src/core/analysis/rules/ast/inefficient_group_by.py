import re
from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_inefficient_group_by(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает неэффективные операции GROUP BY
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие GROUP BY в запросе
        if 'GROUP BY' in query.upper():
            # Проверяем наличие HashAggregate или GroupAggregate в плане выполнения
            if 'plan' in plan:
                plan_content = str(plan['plan'])
                if 'HashAggregate' in plan_content or 'GroupAggregate' in plan_content:
                    # Проверяем, есть ли индекс для группировки
                    has_index_for_group = False
                    if 'indexes' in context and 'group_by_columns' in context:
                        for index in context['indexes']:
                            if 'index_columns' in index:
                                # Простая проверка: если столбцы индекса совпадают со столбцами GROUP BY
                                if set(index['index_columns']) & set(context['group_by_columns']):
                                    has_index_for_group = True
                                    break

                    if not has_index_for_group:
                        # Проверяем количество строк для группировки
                        rows_match = re.search(r'rows=(\d+)', plan_content)
                        if rows_match and int(rows_match.group(1)) > 10000:
                            diagnose = LintDiagnose(
                                line=1,
                                col=1,
                                severity="MEDIUM",
                                message="Обнаружена неэффективная операция группировки большого объема данных",
                                recommendation="Рассмотрите возможность добавления индекса по столбцам, используемым в GROUP BY, или предварительной фильтрации данных перед группировкой."
                            )
                            recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in inefficient_group_by rule: {e}")

    return recommendations, optimized_query #ty: ignore
