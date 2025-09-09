import json
from typing import Dict, List, Any, Optional, Set
from ....models.lint_diagnose import LintDiagnose


def rule_autovacuum_tuning(query: str, plan: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Рекомендации по настройке autovacuum
    """
    recommendations = []

    try:
        # Анализ настроек autovacuum
        db_settings = context.get('settings', {})

        # Рекомендации по autovacuum
        autovacuum_recommendations = analyze_autovacuum_settings(db_settings, context)
        recommendations.extend(autovacuum_recommendations)

        # Рекомендации по autovacuum_vacuum_scale_factor
        autovacuum_vacuum_scale_factor_recommendations = analyze_autovacuum_vacuum_scale_factor_settings(db_settings, context)
        recommendations.extend(autovacuum_vacuum_scale_factor_recommendations)

        # Рекомендации по autovacuum_analyze_scale_factor
        autovacuum_analyze_scale_factor_recommendations = analyze_autovacuum_analyze_scale_factor_settings(db_settings, context)
        recommendations.extend(autovacuum_analyze_scale_factor_recommendations)

        # Рекомендации по autovacuum_vacuum_threshold
        autovacuum_vacuum_threshold_recommendations = analyze_autovacuum_vacuum_threshold_settings(db_settings, context)
        recommendations.extend(autovacuum_vacuum_threshold_recommendations)

        # Рекомендации по autovacuum_analyze_threshold
        autovacuum_analyze_threshold_recommendations = analyze_autovacuum_analyze_threshold_settings(db_settings, context)
        recommendations.extend(autovacuum_analyze_threshold_recommendations)

        # Рекомендации по autovacuum_vacuum_cost_limit
        autovacuum_vacuum_cost_limit_recommendations = analyze_autovacuum_vacuum_cost_limit_settings(db_settings, context)
        recommendations.extend(autovacuum_vacuum_cost_limit_recommendations)

        # Рекомендации по autovacuum_vacuum_cost_delay
        autovacuum_vacuum_cost_delay_recommendations = analyze_autovacuum_vacuum_cost_delay_settings(db_settings, context)
        recommendations.extend(autovacuum_vacuum_cost_delay_recommendations)

        # Рекомендации по autovacuum_analyze_cost_limit
        autovacuum_analyze_cost_limit_recommendations = analyze_autovacuum_analyze_cost_limit_settings(db_settings, context)
        recommendations.extend(autovacuum_analyze_cost_limit_recommendations)

        # Рекомендации по autovacuum_analyze_cost_delay
        autovacuum_analyze_cost_delay_recommendations = analyze_autovacuum_analyze_cost_delay_settings(db_settings, context)
        recommendations.extend(autovacuum_analyze_cost_delay_recommendations)

        # Рекомендации по autovacuum_freeze_max_age
        autovacuum_freeze_max_age_recommendations = analyze_autovacuum_freeze_max_age_settings(db_settings, context)
        recommendations.extend(autovacuum_freeze_max_age_recommendations)

        # Рекомендации по autovacuum_multixact_freeze_max_age
        autovacuum_multixact_freeze_max_age_recommendations = analyze_autovacuum_multixact_freeze_max_age_settings(db_settings, context)
        recommendations.extend(autovacuum_multixact_freeze_max_age_recommendations)

        # Рекомендации по autovacuum_max_workers
        autovacuum_max_workers_recommendations = analyze_autovacuum_max_workers_settings(db_settings, context)
        recommendations.extend(autovacuum_max_workers_recommendations)

        # Рекомендации по autovacuum_naptime
        autovacuum_naptime_recommendations = analyze_autovacuum_naptime_settings(db_settings, context)
        recommendations.extend(autovacuum_naptime_recommendations)

    except Exception as e:
        print(f"Error in autovacuum_tuning rule: {e}")

    return recommendations


def analyze_autovacuum_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum
    """
    recommendations = []

    try:
        autovacuum = db_settings.get('autovacuum', {}).get('setting', 'on')

        # Если autovacuum выключен
        if autovacuum.lower() == 'off':
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="HIGH",
                message="autovacuum выключен, что может приводить к деградации производительности",
                recommendation="Рассуйте включение autovacuum для автоматического обслуживания таблиц и предотвращения деградации производительности"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum settings: {e}")

    return recommendations


def analyze_autovacuum_vacuum_scale_factor_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_vacuum_scale_factor
    """
    recommendations = []

    try:
        autovacuum_vacuum_scale_factor = float(db_settings.get('autovacuum_vacuum_scale_factor', {}).get('setting', '0.2'))

        # Если autovacuum_vacuum_scale_factor слишком большой
        if autovacuum_vacuum_scale_factor > 0.3:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_scale_factor установлен в {autovacuum_vacuum_scale_factor}, что может вызывать запаздывание очистки",
                recommendation="Рассуйте уменьшение autovacuum_vacuum_scale_factor до 0.1-0.2 для более частой очистки таблиц"
            ))

        # Если autovacuum_vacuum_scale_factor слишком маленький
        elif autovacuum_vacuum_scale_factor < 0.05:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_scale_factor установлен в {autovacuum_vacuum_scale_factor}, что может вызывать слишком частую очистку",
                recommendation="Рассуйте увеличение autovacuum_vacuum_scale_factor до 0.1-0.2 для уменьшения нагрузки на систему"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_vacuum_scale_factor settings: {e}")

    return recommendations


def analyze_autovacuum_analyze_scale_factor_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_analyze_scale_factor
    """
    recommendations = []

    try:
        autovacuum_analyze_scale_factor = float(db_settings.get('autovacuum_analyze_scale_factor', {}).get('setting', '0.1'))

        # Если autovacuum_analyze_scale_factor слишком большой
        if autovacuum_analyze_scale_factor > 0.2:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_scale_factor установлен в {autovacuum_analyze_scale_factor}, что может вызывать запаздывание анализа",
                recommendation="Рассуйте уменьшение autovacuum_analyze_scale_factor до 0.05-0.1 для более частого анализа таблиц"
            ))

        # Если autovacuum_analyze_scale_factor слишком маленький
        elif autovacuum_analyze_scale_factor < 0.01:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_scale_factor установлен в {autovacuum_analyze_scale_factor}, что может вызывать слишком частый анализ",
                recommendation="Рассуйте увеличение autovacuum_analyze_scale_factor до 0.05-0.1 для уменьшения нагрузки на систему"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_analyze_scale_factor settings: {e}")

    return recommendations


def analyze_autovacuum_vacuum_threshold_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_vacuum_threshold
    """
    recommendations = []

    try:
        autovacuum_vacuum_threshold = int(db_settings.get('autovacuum_vacuum_threshold', {}).get('setting', '50'))

        # Если autovacuum_vacuum_threshold слишком большой
        if autovacuum_vacuum_threshold > 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_threshold установлен в {autovacuum_vacuum_threshold}, что может вызывать запаздывание очистки",
                recommendation="Рассуйте уменьшение autovacuum_vacuum_threshold до 50 для более частой очистки таблиц"
            ))

        # Если autovacuum_vacuum_threshold слишком маленький
        elif autovacuum_vacuum_threshold < 10:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_threshold установлен в {autovacuum_vacuum_threshold}, что может вызывать слишком частую очистку",
                recommendation="Рассуйте увеличение autovacuum_vacuum_threshold до 50 для уменьшения нагрузки на систему"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_vacuum_threshold settings: {e}")

    return recommendations


def analyze_autovacuum_analyze_threshold_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_analyze_threshold
    """
    recommendations = []

    try:
        autovacuum_analyze_threshold = int(db_settings.get('autovacuum_analyze_threshold', {}).get('setting', '50'))

        # Если autovacuum_analyze_threshold слишком большой
        if autovacuum_analyze_threshold > 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_threshold установлен в {autovacuum_analyze_threshold}, что может вызывать запаздывание анализа",
                recommendation="Рассуйте уменьшение autovacuum_analyze_threshold до 50 для более частого анализа таблиц"
            ))

        # Если autovacuum_analyze_threshold слишком маленький
        elif autovacuum_analyze_threshold < 10:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_threshold установлен в {autovacuum_analyze_threshold}, что может вызывать слишком частый анализ",
                recommendation="Рассуйте увеличение autovacuum_analyze_threshold до 50 для уменьшения нагрузки на систему"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_analyze_threshold settings: {e}")

    return recommendations


def analyze_autovacuum_vacuum_cost_limit_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_vacuum_cost_limit
    """
    recommendations = []

    try:
        autovacuum_vacuum_cost_limit = int(db_settings.get('autovacuum_vacuum_cost_limit', {}).get('setting', '200'))

        # Если autovacuum_vacuum_cost_limit слишком маленький
        if autovacuum_vacuum_cost_limit < 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_cost_limit установлен в {autovacuum_vacuum_cost_limit}, что может замедлять очистку",
                recommendation="Рассуйте увеличение autovacuum_vacuum_cost_limit до 200-1000 для ускорения очистки таблиц"
            ))

        # Если autovacuum_vacuum_cost_limit слишком большой
        elif autovacuum_vacuum_cost_limit > 2000:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_cost_limit установлен в {autovacuum_vacuum_cost_limit}, что может вызывать высокую нагрузку",
                recommendation="Рассуйте уменьшение autovacuum_vacuum_cost_limit до 1000-2000 для предотвращения высокой нагрузки"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_vacuum_cost_limit settings: {e}")

    return recommendations


def analyze_autovacuum_vacuum_cost_delay_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_vacuum_cost_delay
    """
    recommendations = []

    try:
        autovacuum_vacuum_cost_delay = int(db_settings.get('autovacuum_vacuum_cost_delay', {}).get('setting', '20'))

        # Если autovacuum_vacuum_cost_delay слишком маленький
        if autovacuum_vacuum_cost_delay < 10:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_cost_delay установлен в {autovacuum_vacuum_cost_delay}ms, что может вызывать высокую нагрузку",
                recommendation="Рассуйте увеличение autovacuum_vacuum_cost_delay до 20-50ms для уменьшения нагрузки на систему"
            ))

        # Если autovacuum_vacuum_cost_delay слишком большой
        elif autovacuum_vacuum_cost_delay > 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_vacuum_cost_delay установлен в {autovacuum_vacuum_cost_delay}ms, что может замедлять очистку",
                recommendation="Рассуйте уменьшение autovacuum_vacuum_cost_delay до 20-50ms для ускорения очистки таблиц"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_vacuum_cost_delay settings: {e}")

    return recommendations


def analyze_autovacuum_analyze_cost_limit_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_analyze_cost_limit
    """
    recommendations = []

    try:
        autovacuum_analyze_cost_limit = int(db_settings.get('autovacuum_analyze_cost_limit', {}).get('setting', '1000'))

        # Если autovacuum_analyze_cost_limit слишком маленький
        if autovacuum_analyze_cost_limit < 500:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_cost_limit установлен в {autovacuum_analyze_cost_limit}, что может замедлять анализ",
                recommendation="Рассуйте увеличение autovacuum_analyze_cost_limit до 1000-2000 для ускорения анализа таблиц"
            ))

        # Если autovacuum_analyze_cost_limit слишком большой
        elif autovacuum_analyze_cost_limit > 5000:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_cost_limit установлен в {autovacuum_analyze_cost_limit}, что может вызывать высокую нагрузку",
                recommendation="Рассуйте уменьшение autovacuum_analyze_cost_limit до 2000-5000 для предотвращения высокой нагрузки"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_analyze_cost_limit settings: {e}")

    return recommendations


def analyze_autovacuum_analyze_cost_delay_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_analyze_cost_delay
    """
    recommendations = []

    try:
        autovacuum_analyze_cost_delay = int(db_settings.get('autovacuum_analyze_cost_delay', {}).get('setting', '20'))

        # Если autovacuum_analyze_cost_delay слишком маленький
        if autovacuum_analyze_cost_delay < 10:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_cost_delay установлен в {autovacuum_analyze_cost_delay}ms, что может вызывать высокую нагрузку",
                recommendation="Рассуйте увеличение autovacuum_analyze_cost_delay до 20-50ms для уменьшения нагрузки на систему"
            ))

        # Если autovacuum_analyze_cost_delay слишком большой
        elif autovacuum_analyze_cost_delay > 100:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_analyze_cost_delay установлен в {autovacuum_analyze_cost_delay}ms, что может замедлять анализ",
                recommendation="Рассуйте уменьшение autovacuum_analyze_cost_delay до 20-50ms для ускорения анализа таблиц"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_analyze_cost_delay settings: {e}")

    return recommendations


def analyze_autovacuum_freeze_max_age_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_freeze_max_age
    """
    recommendations = []

    try:
        autovacuum_freeze_max_age = int(db_settings.get('autovacuum_freeze_max_age', {}).get('setting', '200000000'))

        # Если autovacuum_freeze_max_age слишком маленький
        if autovacuum_freeze_max_age < 100000000:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_freeze_max_age установлен в {autovacuum_freeze_max_age}, что может вызывать слишком частые заморозки",
                recommendation="Рассуйте увеличение autovacuum_freeze_max_age до 200000000 для уменьшения частоты заморозок"
            ))

        # Если autovacuum_freeze_max_age слишком большой
        elif autovacuum_freeze_max_age > 300000000:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_freeze_max_age установлен в {autovacuum_freeze_max_age}, что может вызывать задержку заморозок",
                recommendation="Рассуйте уменьшение autovacuum_freeze_max_age до 200000000 для предотвращения задержки заморозок"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_freeze_max_age settings: {e}")

    return recommendations


def analyze_autovacuum_multixact_freeze_max_age_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_multixact_freeze_max_age
    """
    recommendations = []

    try:
        autovacuum_multixact_freeze_max_age = int(db_settings.get('autovacuum_multixact_freeze_max_age', {}).get('setting', '400000000'))

        # Если autovacuum_multixact_freeze_max_age слишком маленький
        if autovacuum_multixact_freeze_max_age < 200000000:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_multixact_freeze_max_age установлен в {autovacuum_multixact_freeze_max_age}, что может вызывать слишком частые заморозки",
                recommendation="Рассуйте увеличение autovacuum_multixact_freeze_max_age до 400000000 для уменьшения частоты заморозок"
            ))

        # Если autovacuum_multixact_freeze_max_age слишком большой
        elif autovacuum_multixact_freeze_max_age > 600000000:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_multixact_freeze_max_age установлен в {autovacuum_multixact_freeze_max_age}, что может вызывать задержку заморозок",
                recommendation="Рассуйте уменьшение autovacuum_multixact_freeze_max_age до 400000000 для предотвращения задержки заморозок"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_multixact_freeze_max_age settings: {e}")

    return recommendations


def analyze_autovacuum_max_workers_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_max_workers
    """
    recommendations = []

    try:
        autovacuum_max_workers = int(db_settings.get('autovacuum_max_workers', {}).get('setting', '3'))

        # Если autovacuum_max_workers слишком маленький
        if autovacuum_max_workers < 2:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_max_workers установлен в {autovacuum_max_workers}, что может замедлять обслуживание",
                recommendation="Рассуйте увеличение autovacuum_max_workers до 3-5 для ускорения обслуживания таблиц"
            ))

        # Если autovacuum_max_workers слишком большой
        elif autovacuum_max_workers > 10:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_max_workers установлен в {autovacuum_max_workers}, что может вызывать высокую нагрузку",
                recommendation="Рассуйте уменьшение autovacuum_max_workers до 3-5 для предотвращения высокой нагрузки"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_max_workers settings: {e}")

    return recommendations


def analyze_autovacuum_naptime_settings(db_settings: Dict[str, Any], context: Dict[str, Any]) -> List[LintDiagnose]:
    """
    Анализ настроек autovacuum_naptime
    """
    recommendations = []

    try:
        autovacuum_naptime = int(db_settings.get('autovacuum_naptime', {}).get('setting', '60'))

        # Если autovacuum_naptime слишком большой
        if autovacuum_naptime > 120:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_naptime установлен в {autovacuum_naptime}s, что может замедлять обслуживание",
                recommendation="Рассуйте уменьшение autovacuum_naptime до 60s для ускорения обслуживания таблиц"
            ))

        # Если autovacuum_naptime слишком маленький
        elif autovacuum_naptime < 30:
            recommendations.append(LintDiagnose(
                line=1,
                col=1,
                severity="LOW",
                message=f"autovacuum_naptime установлен в {autovacuum_naptime}s, что может вызывать высокую нагрузку",
                recommendation="Рассуйте увеличение autovacuum_naptime до 60s для предотвращения высокой нагрузки"
            ))

    except Exception as e:
        print(f"Error analyzing autovacuum_naptime settings: {e}")

    return recommendations
