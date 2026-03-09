#!/usr/bin/env python3
"""
LLM Explainer - Generates text explanations for data quality findings
"""

import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ.pop('REQUESTS_CA_BUNDLE', None)
os.environ.pop('CURL_CA_BUNDLE', None)
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['CURL_CA_BUNDLE'] = ''

import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean

# Azure OpenAI configuration
API_KEY = "580d87fc2e114ce6b484e72334dc84e9"
API_VERSION = "2023-07-01-preview"
AZURE_ENDPOINT = "https://dr-ai-dev-1001.openai.azure.com/"
DEPLOYMENT_NAME = "msgen4o"

def load_report(filepath="final_report.json"):
    """Load the final report JSON."""
    with open(filepath, 'r') as f:
        return json.load(f)

def calculate_dimension_scores(report):
    """Calculate average score per dimension."""
    dimension_scores = {}
    
    for dimension, findings in report.items():
        if not findings:
            dimension_scores[dimension] = 100
            continue
        
        percentages = [f.get("percentage", 100) for f in findings]
        dimension_scores[dimension] = round(mean(percentages), 2)
    
    return dimension_scores

def call_llm_api(messages, max_tokens=1024):
    """Make API call to Azure OpenAI."""
    url = f"{AZURE_ENDPOINT}/openai/deployments/{DEPLOYMENT_NAME}/chat/completions?api-version={API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY
    }
    payload = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=180, verify=False)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()

def analyze_chunk(dimension_name, chunk, chunk_index):
    """Analyze a chunk of findings."""
    findings_summary = json.dumps(chunk, indent=2)
    prompt = f"""Analyze {dimension_name} data quality findings (chunk {chunk_index}):

{findings_summary}

Provide brief analysis covering status, issues, and impact."""
    
    messages = [
        {"role": "system", "content": "You are a data quality analyst. Provide clear insights."},
        {"role": "user", "content": prompt}
    ]
    
    return call_llm_api(messages, max_tokens=1024)

def merge_chunk_analyses(dimension_name, chunk_analyses):
    """Merge multiple chunk analyses into one comprehensive analysis."""
    combined = "\n\n".join([f"Analysis {i+1}:\n{analysis}" for i, analysis in enumerate(chunk_analyses)])
    
    prompt = f"""Merge these {len(chunk_analyses)} analyses for {dimension_name} into one comprehensive summary:

{combined}

Provide a unified analysis covering overall status, key issues, and business impact."""
    
    messages = [
        {"role": "system", "content": "You are a data quality analyst. Synthesize multiple analyses into one coherent summary."},
        {"role": "user", "content": prompt}
    ]
    
    return call_llm_api(messages, max_tokens=1024)

def format_with_llm(raw_explanation, dimension_name):
    """Second LLM call to format output into business-focused insights."""
    prompt = f"""Convert this data quality analysis into a concise business insight:

{raw_explanation}

Format as:
**{dimension_name}**
• [Key Point 1 - business impact focused]
• [Key Point 2 - actionable recommendation]

Keep it under 50 words total. Be specific and actionable."""
    
    messages = [
        {"role": "system", "content": "You are a business analyst. Convert technical findings into clear business insights with exactly 2 bullet points."},
        {"role": "user", "content": prompt}
    ]
    
    return call_llm_api(messages, max_tokens=150)

def call_llm(dimension_name, findings):
    """Call LLM to explain findings for a dimension - single optimized call."""
    try:
        print(f"  Processing {dimension_name}: {len(findings)} findings")
        
        # Summarize findings into key stats
        failed = [f for f in findings if f.get('status') == 'failed']
        columns = [f.get('column_name') for f in failed[:10]]
        
        summary = {
            'total_findings': len(findings),
            'failed_count': len(failed),
            'sample_columns': columns
        }
        
        prompt = f"""Analyze {dimension_name} data quality:

Stats: {len(failed)}/{len(findings)} columns failed
Sample columns: {', '.join(columns[:5])}

Provide 2 bullet points:
• Business impact
• Recommended action

Keep under 40 words total."""
        
        messages = [
            {"role": "system", "content": "You are a data quality analyst. Provide concise, actionable insights."},
            {"role": "user", "content": prompt}
        ]
        
        explanation = call_llm_api(messages, max_tokens=150)
        
        print(f"[OK] Completed: {dimension_name}")
        return {
            "dimension": dimension_name,
            "explanation": explanation,
            "findings_count": len(findings)
        }
        
    except Exception as e:
        print(f"[ERROR] Error in {dimension_name}: {str(e)}")
        return {
            "dimension": dimension_name,
            "explanation": f"**{dimension_name}**\n• {len(findings)} findings detected\n• Review required",
            "findings_count": len(findings)
        }

def generate_explanations(report):
    """Generate explanations for all dimensions using parallel processing."""
    explanations = []
    
    print(f"\n{'='*60}")
    print("Generating LLM Explanations (8 dimensions in parallel)")
    print(f"{'='*60}\n")
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_dimension = {
            executor.submit(call_llm, dimension, findings): dimension 
            for dimension, findings in report.items() if findings
        }
        
        for future in as_completed(future_to_dimension):
            result = future.result()
            explanations.append(result)
    
    print(f"\n[OK] All {len(explanations)} dimensions processed\n")
    return explanations

def extract_column_metrics(dimension, findings):
    """Extract column-specific metrics based on dimension type."""
    column_metrics = {}
    
    metric_mapping = {
        'Completeness': 'total_null',
        'Accuracy': 'outlier_count',
        'Availability': 'response_time',
        'Timeliness': 'check_type',
        'Uniqueness': 'duplicate_groups',
        'Usability': 'issue_type',
        'Validity': 'score',
        'Consistency': 'score'
    }
    
    metric_key = metric_mapping.get(dimension, 'value')
    
    for finding in findings:
        col_name = finding.get('column_name')
        if col_name:
            metric_value = finding.get(metric_key, finding.get('value', 0))
            column_metrics[col_name] = metric_value
    
    return column_metrics

def generate_explanations_from_data(report):
    """Generate explanations from report data passed directly."""
    dimension_scores = calculate_dimension_scores(report)
    overall_score = round(mean(dimension_scores.values()), 2)
    
    print(f"\n{'='*60}")
    print("DIMENSION SCORES")
    print(f"{'='*60}\n")
    print(json.dumps(dimension_scores, indent=2))
    print(f"\nOverall Quality Score: {overall_score}")
    
    explanations = generate_explanations(report)
    
    for exp in explanations:
        dimension = exp['dimension']
        findings = report.get(dimension, [])
        exp['column_metrics'] = extract_column_metrics(dimension, findings)
    
    explanations.sort(key=lambda x: x["dimension"])
    
    print(f"\n{'='*60}")
    print("DATA QUALITY EXPLANATIONS")
    print(f"{'='*60}\n")
    
    for exp in explanations:
        dim_score = dimension_scores.get(exp['dimension'], 0)
        print(f" {exp['dimension'].upper()} (Score: {dim_score}% | {exp['findings_count']} findings)")
        print(f"{'-'*60}")
        print(exp['explanation'])
        print(f"\n{'='*60}\n")
    
    output = {
        "overall_quality_score": overall_score,
        "dimension_scores": dimension_scores,
        "explanations": explanations,
        "total_dimensions": len(explanations)
    }
    
    with open("compressor/llm_explanations.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(" Explanations saved to compressor/llm_explanations.json")
    
    print(f"\n{'='*60}")
    print("Calling Recommendation Agent...")
    print(f"{'='*60}\n")
    call_recommendation_agent(output)
    
    return output

def call_recommendation_agent(compressor_output):
    """Call recommendation agent with timeout handling."""
    import subprocess
    try:
        result = subprocess.run(
            ["python", "recommendation/recommendation_agent.py"],
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode == 0:
            print(result.stdout)
            print("\n" + "="*60)
            print("[OK] Recommendation agent completed successfully")
            print("="*60 + "\n")
        else:
            print(f"\n[ERROR] Recommendation agent error: {result.stderr}\n")
    except subprocess.TimeoutExpired:
        print("\n[WARNING] Recommendation agent timeout - continuing with healing...\n")
    except Exception as e:
        print(f"\n[ERROR] Error calling recommendation agent: {str(e)}\n")

def main():
    """Main execution flow - for standalone usage."""
    print("Loading data quality report...")
    report = load_report()
    generate_explanations_from_data(report)

if __name__ == "__main__":
    main()
