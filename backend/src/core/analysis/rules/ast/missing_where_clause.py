import sqlparse
from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_missing_where_clause(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает запросы без WHERE clauses, которые могут сканировать всю таблицу
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие WHERE в запросе
        parsed = sqlparse.parse(query)
        for statement in parsed:
            has_where = False
            for token in statement.tokens:
                if hasattr(token, 'ttype') and token.ttype is sqlparse.tokens.Keyword and str(token).strip().upper() == 'WHERE':
                    has_where = True
                    break

            if not has_where and 'SELECT' in str(statement).upper():
                # Проверяем, является ли это запросом к одной таблице
                if 'plan' in plan:
                    plan_content = str(plan['plan'])
                    if 'Seq Scan' in plan_content:
                        table_match = re.search(r'Seq Scan on (\w+)', plan_content)
                        if table_match:
                            table_name = table_match.group(1)

                            # Проверяем размер таблицы
                            if 'table_sizes' in context and table_name in context['table_sizes']:
                                size = context['table_sizes'][table_name]
                                if size > 10000:  # Если таблица большая
                                    diagnose = LintDiagnose(
                                        line=1,
                                        col=1,
                                        severity="HIGH",
                                        message=f"Запрос к большой таблице {table_name} без условия WHERE",
                                        recommendation=f"Добавьте условие WHERE для ограничения количества возвращаемых строк из таблицы {table_name}. Без условия WHERE запрос сканирует всю таблицу, что неэффективно для больших таблиц."
                                    )
                                    recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in missing_where_clause rule: {e}")

    return recommendations, optimized_query #ty: ignore

