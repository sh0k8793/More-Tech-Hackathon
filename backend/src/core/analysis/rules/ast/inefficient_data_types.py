from core.models.lint_diagnose import LintDiagnose


from typing import Any, Dict, List


def rule_inefficient_data_types(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Обнаруживает использование неэффективных типов данных
    """
    recommendations = []
    optimized_query = query

    try:
        # Проверяем наличие операций сравнения с неэффективными типами данных
        if 'plan' in plan:
            plan_content = str(plan['plan'])

            # Проверяем наличие приведения типов
            if 'Type Cast' in plan_content:
                diagnose = LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message="Обнаружено приведение типов в запросе",
                    recommendation="Избегайте приведения типов в запросах, особенно в условиях WHERE и JOIN. Используйте одинаковые типы данных для сравниваемых столбцов или измените схему базы данных для использования более эффективных типов данных."
                )
                recommendations.append(diagnose)

            # Проверяем использование VARCHAR без указания длины
            if 'schema' in context:
                for table in context['schema']:
                    for column in table['columns']:
                        if column['type'].upper() == 'VARCHAR' and '(' not in column['type']:
                            diagnose = LintDiagnose(
                                line=1,
                                col=1,
                                severity="LOW",
                                message=f"Обнаружен столбец {column['name']} в таблице {table['name']} с типом VARCHAR без указания длины",
                                recommendation=f"Укажите длину для столбца {column['name']} (например, VARCHAR(255)) или рассмотрите возможность использования TEXT для больших объемов текста."
                            )
                            recommendations.append(diagnose)
    except Exception as e:
        print(f"Error in inefficient_data_types rule: {e}")

    return recommendations, optimized_query #ty: ignore
