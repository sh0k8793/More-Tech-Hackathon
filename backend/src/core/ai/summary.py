from core.models.analysis_result import AnalysisResult
from core.ai.client import get_client
from core.ai.settings import settings
from jinja2 import Environment, FileSystemLoader
import json
import os

template_dir = os.path.join(os.path.dirname(__file__), 'templates')
env = Environment(loader=FileSystemLoader(template_dir))


def get_ai_summary(analysis_result: AnalysisResult) -> str:
    """
    Generate AI summary based on analysis result using OpenAI API.

    Args:
        analysis_result: AnalysisResult containing lint diagnoses

    Returns:
        str: AI-generated summary recommendation
    """
    if not analysis_result.lint_diagnoses:
        return "No issues found in the code analysis."

    # Load the template from external file

    template = env.get_template('summary.jinja2')

    # Prepare data for template
    errors_data = []
    for diagnose in analysis_result.lint_diagnoses:
        errors_data.append({
            'line': diagnose.line,
            'col': diagnose.col,
            'severity': diagnose.severity,
            'message': diagnose.message,
            'recommendation': diagnose.recommendation
        })

    # Render template
    prompt = template.render(errors=errors_data)

    try:
        # Get AI client
        client = get_client()

        # Create completion
        response = client.chat.completions.create(
            model=settings.OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": "You are an expert SQL code analyst. Provide concise, actionable recommendations for fixing lint issues."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.7,
            think=False # Если не сработает удалите и поставьте модель qwen2.5:7b !!!
        )

        # Extract and return the summary
        summary = response.choices[0].message.content.strip()
        return summary

    except Exception as e:
        # Fallback summary if AI service is unavailable
        error_count = len(analysis_result.lint_diagnoses)
        high_severity = sum(1 for d in analysis_result.lint_diagnoses if d.severity == "HIGH")
        medium_severity = sum(1 for d in analysis_result.lint_diagnoses if d.severity == "MEDIUM")
        low_severity = sum(1 for d in analysis_result.lint_diagnoses if d.severity == "LOW")

        fallback_summary = f"""Code Analysis Summary:
- Total issues found: {error_count}
- High severity: {high_severity}
- Medium severity: {medium_severity}
- Low severity: {low_severity}

Review the specific issues above and prioritize fixing high-severity problems first."""

        return fallback_summary
