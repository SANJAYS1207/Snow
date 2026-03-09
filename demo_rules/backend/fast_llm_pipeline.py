#!/usr/bin/env python3
"""
Fast LLM Pipeline - Single API call for analysis, recommendations, and healing
"""
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['CURL_CA_BUNDLE'] = ''

import json
import requests
from statistics import mean

API_KEY = "580d87fc2e114ce6b484e72334dc84e9"
API_VERSION = "2023-07-01-preview"
AZURE_ENDPOINT = "https://dr-ai-dev-1001.openai.azure.com/"
DEPLOYMENT_NAME = "msgen4o"

def call_llm(messages, max_tokens=3000):
    url = f"{AZURE_ENDPOINT}/openai/deployments/{DEPLOYMENT_NAME}/chat/completions?api-version={API_VERSION}"
    headers = {"Content-Type": "application/json", "api-key": API_KEY}
    payload = {"messages": messages, "max_tokens": max_tokens, "temperature": 0.3}
    response = requests.post(url, headers=headers, json=payload, timeout=120, verify=False)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()

def calculate_scores(report):
    dimension_scores = {}
    for dimension, findings in report.items():
        if not findings:
            dimension_scores[dimension] = 100
        else:
            percentages = [f.get("percentage", 100) for f in findings]
            dimension_scores[dimension] = round(mean(percentages), 2)
    return dimension_scores

def generate_all_in_one(report):
    """Single LLM call for everything"""
    dimension_scores = calculate_scores(report)
    overall_score = round(mean(dimension_scores.values()), 2)
    
    context = f"Data Quality Score: {overall_score}%\nTarget: 90%\n\n"
    for dim, score in dimension_scores.items():
        findings = report.get(dim, [])
        failed = [f for f in findings if f.get('status') == 'failed']
        context += f"{dim}: {score}% ({len(failed)}/{len(findings)} failed)\n"
    
    prompt = f"""{context}

Generate recommendations for each dimension with:
- **Approach**: Fix method
- **Criticality**: CRITICAL/HIGH/MEDIUM/LOW
- **Effort**: LOW/MEDIUM/HIGH
- **Expected Improvement**: X%
- **SQL Fixable**: Yes/No/Partially
- **Columns**: List affected columns
- **Reason**: Why these columns

Format as markdown with ### headers for each dimension."""
    
    messages = [
        {"role": "system", "content": "You are a data quality expert. Provide structured recommendations."},
        {"role": "user", "content": prompt}
    ]
    
    print("Calling LLM for complete analysis...")
    recommendations = call_llm(messages, max_tokens=3000)
    
    return {
        "overall_quality_score": overall_score,
        "dimension_scores": dimension_scores,
        "explanations": [{"dimension": d, "explanation": f"{d}: {s}%", "findings_count": len(report.get(d, []))} 
                        for d, s in dimension_scores.items()],
        "total_dimensions": len(dimension_scores)
    }, {
        "current_score": overall_score,
        "target_score": 90.0,
        "improvement_needed": round(90.0 - overall_score, 2),
        "recommendations": recommendations
    }

def main():
    with open("final_report.json", "r") as f:
        report = json.load(f)
    
    print("\n" + "="*70)
    print("FAST LLM PIPELINE - Single API Call")
    print("="*70 + "\n")
    
    llm_output, rec_output = generate_all_in_one(report)
    
    with open("compressor/llm_explanations.json", "w") as f:
        json.dump(llm_output, f, indent=2)
    print("[OK] LLM explanations saved")
    
    with open("recommendation/self_healing_input.json", "w") as f:
        json.dump(rec_output, f, indent=2)
    print("[OK] Recommendations saved")
    
    print("\n" + "="*70 + "\n")

if __name__ == "__main__":
    main()
