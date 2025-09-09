from typing import Any, Dict, List

from core.models.lint_diagnose import LintDiagnose


def rule_n_plus_one_query(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает проблему N+1 запроса
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие множественных запросов к одной таблице
        if 'queries' in context and len(context['queries']) > 1:
            table_queries = {}
            for q in context['queries']:
                if 'table' in q:
                    table = q['table']
                    if table not in table_queries:
                        table_queries[table] = 0
                    table_queries[table] += 1

            for table, count in table_queries.items():
                if count > 1:
                    diagnose = LintDiagnose(
                        line=1,
                        col=1,
                        severity="HIGH",
                        message=f"Обнаружена потенциальная проблема N+1 для таблицы {table}",
                        recommendation=f"Используйте JOIN или подзапросы для получения всех необходимых данных за один запрос вместо выполнения {count} отдельных запросов к таблице {table}."
                    )
                    recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in n_plus_one_query rule: {e}")

    return recommendations, optimized_query #ty: ignore

