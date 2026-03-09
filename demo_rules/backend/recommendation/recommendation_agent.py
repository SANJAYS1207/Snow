#!/usr/bin/env python3
"""
Recommendation Agent - Suggests approaches, criticality, effort, and improvement per dimension
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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = "580d87fc2e114ce6b484e72334dc84e9"
API_VERSION = "2023-07-01-preview"
AZURE_ENDPOINT = "https://dr-ai-dev-1001.openai.azure.com/"
DEPLOYMENT_NAME = "msgen4o"

DIMENSION_KNOWLEDGE = {
    "Completeness": {"issue": "missing values and null entries", "reason": "Null values present"},
    "Uniqueness": {"issue": "duplicate values and primary key violations", "reason": "Duplicate records found"},
    "Accuracy": {"issue": "outliers and business rule validation failures", "reason": "Outliers detected"},
    "Consistency": {"issue": "cross-field integrity and format inconsistencies", "reason": "Inconsistent data formats"},
    "Timeliness": {"issue": "data freshness and staleness issues", "reason": "Stale data detected"},
    "Validity": {"issue": "data types, formats and domain constraint violations", "reason": "Invalid data formats"},
    "Usability": {"issue": "accessibility and naming convention issues", "reason": "Data quality issues"},
    "Availability": {"issue": "data access and performance issues", "reason": "Access issues detected"}
}

def call_llm(messages, max_tokens=1024):
    """Make API call to Azure OpenAI"""
    url = f"{AZURE_ENDPOINT}/openai/deployments/{DEPLOYMENT_NAME}/chat/completions?api-version={API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY
    }
    payload = {
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=60, verify=False)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip()

def generate_recommendation_for_dimension(dimension, score, columns, overall_score):
    """Generate recommendation for a single dimension in parallel"""
    try:
        start = time.time()
        print(f"  [{time.strftime('%H:%M:%S')}] Processing {dimension}...")
        knowledge = DIMENSION_KNOWLEDGE.get(dimension, {})
        
        prompt = f"""{dimension} ({score}% score, target 90%):
{knowledge.get('issue', '')} in {len(columns)} columns.

Provide:
- Approach: [fix method]
- Criticality: [CRITICAL/HIGH/MEDIUM/LOW]
- Effort: [LOW/MEDIUM/HIGH]
- Expected Improvement: [X%]
- SQL Fixable: [Yes/No/Partially]
- Columns: [{', '.join(columns[:20])}]
- Reason: [brief reason]"""
        
        messages = [
            {"role": "system", "content": "Data quality expert. Be concise."},
            {"role": "user", "content": prompt}
        ]
        
        result = call_llm(messages, max_tokens=300)
        elapsed = time.time() - start
        print(f"[OK] Completed: {dimension} in {elapsed:.1f}s")
        return f"### {dimension}\n{result}"
        
    except Exception as e:
        elapsed = time.time() - start
        print(f"[ERROR] {dimension} failed after {elapsed:.1f}s: {str(e)}")
        return f"### {dimension}\n- Approach: Review and fix {knowledge.get('issue', 'issues')}\n- Criticality: MEDIUM\n- Effort: MEDIUM\n- Expected Improvement: 5%\n- SQL Fixable: Partially\n- Columns: {', '.join(columns[:20])}\n- Reason: {knowledge.get('reason', 'Issues detected')}"

def generate_recommendations(compressor_output):
    """Generate recommendations with parallel processing"""
    
    overall_score = compressor_output["overall_quality_score"]
    dimension_scores = compressor_output["dimension_scores"]
    explanations = compressor_output["explanations"]
    
    start_time = time.time()
    print(f"\n[{time.strftime('%H:%M:%S')}] Generating recommendations (parallel processing)...\n")
    
    dimension_recommendations = []
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for exp in explanations:
            dimension = exp["dimension"]
            score = dimension_scores.get(dimension, 0)
            columns = list(exp.get("column_metrics", {}).keys())
            
            future = executor.submit(
                generate_recommendation_for_dimension,
                dimension, score, columns, overall_score
            )
            futures.append(future)
        
        for future in as_completed(futures):
            result = future.result()
            dimension_recommendations.append(result)
    
    elapsed = time.time() - start_time
    print(f"\n[OK] All {len(dimension_recommendations)} dimensions processed in {elapsed:.1f}s\n")
    
    recommendations = "\n\n".join(sorted(dimension_recommendations))
    recommendations += "\n\n[SUMMARY]\nAddress high-criticality dimensions first to maximize quality improvement.\n\n[EXECUTION PRIORITY]\n1. CRITICAL items\n2. HIGH items\n3. MEDIUM items\n4. LOW items"
    
    return recommendations

def main():
    """Main execution"""
    
    with open("compressor/llm_explanations.json", "r") as f:
        compressor_output = json.load(f)
    
    print("\n" + "="*70)
    print("RECOMMENDATION AGENT - Approach & Effort Analysis")
    print("="*70)
    
    recommendations = generate_recommendations(compressor_output)
    
    print("\n" + "="*70)
    print("RECOMMENDATIONS FOR SELF-HEALING AGENT")
    print("="*70 + "\n")
    print(recommendations)
    print("\n" + "="*70)
    
    output = {
        "current_score": compressor_output["overall_quality_score"],
        "target_score": 90.0,
        "improvement_needed": round(90.0 - compressor_output["overall_quality_score"], 2),
        "recommendations": recommendations
    }
    
    with open("recommendation/self_healing_input.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print("\n[OK] Output saved to: recommendation/self_healing_input.json")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
