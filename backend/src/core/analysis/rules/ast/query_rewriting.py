import sqlparse
import re
from typing import Dict, List, Any, Optional, Set, Tuple
from ....models.lint_diagnose import LintDiagnose


def rule_query_rewriting(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Рекомендации по переписыванию текста запросов
    """
    recommendations = []

    try:
        # Анализ на возможность использования EXISTS вместо IN
        exists_vs_in_recommendations = analyze_exists_vs_in(query)
        recommendations.extend(exists_vs_in_recommendations)

        # Анализ на возможность использования JOIN вместо подзапросов
        join_vs_subquery_recommendations = analyze_join_vs_subquery(query)
        recommendations.extend(join_vs_subquery_recommendations)

        # Анализ на возможность использования CTE
        cte_recommendations = analyze_cte_opportunities(query)
        recommendations.extend(cte_recommendations)

        # Анализ на возможность использования оконных функций
        window_function_recommendations = analyze_window_function_opportunities(query)
        recommendations.extend(window_function_recommendations)

        # Анализ на возможность использования DISTINCT вместо GROUP BY
        distinct_vs_groupby_recommendations = analyze_distinct_vs_groupby(query)
        recommendations.extend(distinct_vs_groupby_recommendations)

        # Анализ на возможность использования LIMIT вместо подзапросов
        limit_vs_subquery_recommendations = analyze_limit_vs_subquery(query)
        recommendations.extend(limit_vs_subquery_recommendations)

        # Анализ на возможность использования UNION ALL вместо UNION
        union_all_recommendations = analyze_union_opportunities(query)
        recommendations.extend(union_all_recommendations)

    except Exception as e:
        print(f"Error in query_rewriting rule: {e}")

    return recommendations


def analyze_exists_vs_in(query: str) -> List[LintDiagnose]:
    """
    Анализ возможности использования EXISTS вместо IN
    """
    recommendations = []

    try:
        # Ищем конструкции IN с подзапросами
        in_patterns = re.findall(r'where\s+\w+\s+in\s*\(\s*select', query, re.IGNORECASE | re.DOTALL)

        for pattern in in_patterns:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message="Обнаружено использование IN с подзапросом",
                recommendation="Рассуйте использование EXISTS вместо IN для лучшей производительности, особенно когда подзапрос возвращает много строк"
            ))

        # Ищем конструкции IN с большим количеством значений
        in_values_patterns = re.findall(r'where\s+\w+\s+in\s*\(([^)]+)\)', query, re.IGNORECASE | re.DOTALL)
        for pattern in in_values_patterns:
            values = [v.strip() for v in pattern.split(',') if v.strip()]
            if len(values) > 5:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message=f"IN с большим количеством значений ({len(values)})",
                    recommendation="Рассуйте использование JOIN вместо IN с большим количеством значений для лучшей производительности"
                ))

    except Exception as e:
        print(f"Error analyzing EXISTS vs IN: {e}")

    return recommendations


def analyze_join_vs_subquery(query: str) -> List[LintDiagnose]:
    """
    Анализ возможности использования JOIN вместо подзапросов
    """
    recommendations = []

    try:
        # Ищем вложенные подзапросы
        subquery_patterns = [
            r'select.*?from.*?where.*?in\s*\(\s*select',
            r'select.*?from.*?where.*?exists\s*\(\s*select',
            r'select.*?from.*?where.*?not\s+exists\s*\(\s*select'
        ]

        for pattern in subquery_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE | re.DOTALL)
            if matches:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message="Обнаружен подзапрос, который может быть преобразован в JOIN",
                    recommendation="Рассуйте преобразование подзапроса в JOIN для улучшения производительности и читаемости"
                ))

        # Ищем коррелированные подзапросы
        correlated_patterns = [
            r'select.*?from.*?where.*?exists\s*\(\s*select.*?\b\w+\.\w+\s*=',
            r'select.*?from.*?where.*?not\s+exists\s*\(\s*select.*?\b\w+\.\w+\s*='
        ]

        for pattern in correlated_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE | re.DOTALL)
            if matches:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="HIGH",
                    message="Обнаружен коррелированный подзапрос - потенциальный источник производительности",
                    recommendation="Рассуйте преобразование коррелированного подзапроса в JOIN для значительного улучшения производительности"
                ))

    except Exception as e:
        print(f"Error analyzing JOIN vs subquery: {e}")

    return recommendations


def analyze_cte_opportunities(query: str) -> List[LintDiagnose]:
    """
    Анализ возможностей использования CTE (Common Table Expressions)
    """
    recommendations = []

    try:
        # Ищем сложные подзапросы
        complex_subquery_pattern = r'select.*?from.*?\(select.*?\) as'
        matches = re.findall(complex_subquery_pattern, query, re.IGNORECASE | re.DOTALL)

        if matches:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="MEDIUM",
                message="Обнаружены сложные подзапросы, которые могут быть преобразованы в CTE",
                recommendation="Рассуйте использование CTE (WITH clauses) для улучшения читаемости и возможности повторного использования подзапросов"
            ))

        # Ищем множественные подзапросы
        subquery_count = len(re.findall(r'\(select', query, re.IGNORECASE))
        if subquery_count > 2:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"Обнаружено {subquery_count} подзапросов",
                recommendation="Рассуйте использование CTE для организации сложных запросов с множественными подзапросами"
            ))

    except Exception as e:
        print(f"Error analyzing CTE opportunities: {e}")

    return recommendations


def analyze_window_function_opportunities(query: str) -> List[LintDiagnose]:
    """
    Анализ возможностей использования оконных функций
    """
    recommendations = []

    try:
        # Ищем GROUP BY с агрегатами, которые могут быть заменены оконными функциями
        groupby_patterns = re.findall(r'group\s+by\s+([^)]+)', query, re.IGNORECASE | re.DOTALL)

        for pattern in groupby_patterns:
            # Если есть агрегатные функции без необходимости группировки
            if re.search(r'count\(|sum\(|avg\(|max\(|min\(', pattern, re.IGNORECASE):
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message="Обнаружен GROUP BY с агрегатными функциями",
                    recommendation="Рассуйте использование оконных функций вместо GROUP BY для сохранения строк в результате"
                ))

        # Ищем ранжирование, которое может быть сделано через оконные функции
        ranking_patterns = [
            r'row_number\(\)|rank\(\)|dense_rank\(\)',
            r'lead\(|lag\(|first_value\(|last_value\('
        ]

        for pattern in ranking_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="LOW",
                    message="Обнаружены функции, которые могут быть реализованы через оконные функции",
                    recommendation="Рассуйте использование оконных функций для более эффективной обработки ранжирования и смещенных значений"
                ))

    except Exception as e:
        print(f"Error analyzing window function opportunities: {e}")

    return recommendations


def analyze_distinct_vs_groupby(query: str) -> List[LintDiagnose]:
    """
    Анализ возможности использования DISTINCT вместо GROUP BY
    """
    recommendations = []

    try:
        # Ищем GROUP BY без агрегатных функций
        groupby_patterns = re.findall(r'group\s+by\s+([^)]+)', query, re.IGNORECASE | re.DOTALL)

        for pattern in groupby_patterns:
            # Проверяем, есть ли агрегатные функции в SELECT
            select_pattern = r'select\s+(.*?)\s+from'
            select_match = re.search(select_pattern, query, re.IGNORECASE | re.DOTALL)

            if select_match:
                select_clause = select_match.group(1)
                has_aggregates = re.search(r'count\(|sum\(|avg\(|max\(|min\(', select_clause, re.IGNORECASE)

                if not has_aggregates:
                    recommendations.append(LintDiagnose(
                        line=1,
                        col=1,
                        severity="LOW",
                        message="Обнаружен GROUP BY без агрегатных функций",
                        recommendation="Рассуйте использование DISTINCT вместо GROUP BY для более простого синтаксиса"
                    ))

    except Exception as e:
        print(f"Error analyzing DISTINCT vs GROUP BY: {e}")

    return recommendations


def analyze_limit_vs_subquery(query: str) -> List[LintDiagnose]:
    """
    Анализ возможности использования LIMIT вместо подзапросов
    """
    recommendations = []

    try:
        # Ищем подзапросы для ограничения количества строк
        limit_subquery_patterns = [
            r'select.*?from.*?where.*?id\s+in\s*\(\s*select.*?limit',
            r'select.*?from.*?where.*?exists\s*\(\s*select.*?limit'
        ]

        for pattern in limit_subquery_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE | re.DOTALL)
            if matches:
                recommendations.append(LintDiagnose(
                    line=1,
                    col=1,
                    severity="MEDIUM",
                    message="Обнаружен подзапрос с LIMIT, который может быть оптимизирован",
                    recommendation="Рассуйте использование LIMIT напрямую в основном запросе вместо подзапроса для лучшей производительности"
                ))

    except Exception as e:
        print(f"Error analyzing LIMIT vs subquery: {e}")

    return recommendations


def analyze_union_opportunities(query: str) -> List[LintDiagnose]:
    """
    Анализ возможностей использования UNION ALL вместо UNION
    """
    recommendations = []

    try:
        # Ищем UNION без ALL
        union_patterns = re.findall(r'\bunion\b(?!\s+all)', query, re.IGNORECASE)

        for pattern in union_patterns:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message="Обнаружен UNION без ALL",
                recommendation="Рассуйте использование UNION ALL вместо UNION, если дубликаты не удаляются, для лучшей производительности"
            ))

        # Ищем множественные UNION
        union_count = len(re.findall(r'\bunion\b', query, re.IGNORECASE))
        if union_count > 2:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"Обнаружено {union_count} UNION операций",
                recommendation="Рассуйте использование CTE для организации множественных UNION операций для лучшей читаемости"
            ))

    except Exception as e:
        print(f"Error analyzing UNION opportunities: {e}")

    return recommendations
