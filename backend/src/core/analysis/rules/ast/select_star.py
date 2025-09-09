from typing import Any, Dict, List

import sqlparse

from core.models.lint_diagnose import LintDiagnose


def rule_select_star(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает использование SELECT * в запросах
    """
    recommendations = []
    optimized_query = query

    try:
        parsed = sqlparse.parse(query)

        for statement in parsed:
            for token in statement.tokens:
                if (hasattr(token, 'ttype') and
                    token.ttype is sqlparse.tokens.Wildcard and
                    str(token).strip() == "*"):


                    col = 1

                    if hasattr(statement, 'token_index'):
                        token_index = statement.token_index(token)
                        if token_index != -1:
                            col = query.find('*', max(0, query.find('select') + 6))
                            if col == -1:
                                col = 1

                    diagnose = LintDiagnose(
                        line=1,
                        col=col,
                        severity="MEDIUM",
                        message="Обнаружено использование SELECT * в запросе",
                        recommendation="Явно укажите необходимые колонки вместо использования *. Это улучшит производительность и сделает запрос более понятным."
                    )
                    recommendations.append(diagnose)
                    # Здесь надо переписывать запрос если это возможно, в конкретном примере нет, просто возвращаем исходный запрос
                    break

    except Exception as e:
        print(f"Error in select_star rule: {e}")

    return recommendations, optimized_query #ty: ignore
