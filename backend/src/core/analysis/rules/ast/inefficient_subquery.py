from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_inefficient_subquery(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает неэффективные подзапросы
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие подзапросов в запросе
        if '(' in query and ')' in query:
            # Проверяем наличие подзапросов в плане выполнения
            if 'plan' in plan:
                plan_content = str(plan['plan'])

                # Проверяем наличие коррелированных подзапросов
                if 'SubPlan' in plan_content:
                    # Проверяем, выполняется ли подзапрос для каждой строки основного запроса
                    if 'InitPlan' not in plan_content:
                        diagnose = LintDiagnose(
                            line=1,
                            col=1,
                            severity="HIGH",
                            message="Обнаружен потенциально неэффективный коррелированный подзапрос",
                            recommendation="Рассмотрите возможность замены коррелированного подзапроса на JOIN или переписывания запроса для избежания выполнения подзапроса для каждой строки основного запроса."
                        )
                        recommendations.append(diagnose)

                # Проверяем наличие множественных подзапросов
                subquery_count = plan_content.count('SubPlan')
                if subquery_count > 2:
                    diagnose = LintDiagnose(
                        line=1,
                        col=1,
                        severity="MEDIUM",
                        message=f"Обнаружено множество подзапросов ({subquery_count}) в одном запросе",
                        recommendation="Рассмотрите возможность упрощения запроса или использования временных таблиц (CTE) для уменьшения сложности запроса."
                    )
                    recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in inefficient_subquery rule: {e}")

    return recommendations, optimized_query #ty: ignore
