#!/usr/bin/env python3
"""
Compressor Agent - Multi-dimensional Data Quality Analyzer
Analyzes quality reports from 8 dimensions and generates prioritized insights.
"""

import json
from collections import defaultdict
from statistics import mean

# Weighting constants
WEIGHT_CRITICALITY = 0.5
WEIGHT_BUSINESS_IMPACT = 0.3
WEIGHT_DATA_VOLUME = 0.2

SEVERITY_MAP = {"low": 1, "medium": 2, "high": 3}
IMPACT_MAP = {"low": 1, "medium": 2, "high": 3}


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
        
        # Extract percentage values (all dimensions have percentage field)
        percentages = [f.get("percentage", 100) for f in findings]
        dimension_scores[dimension] = round(mean(percentages), 2)
    
    return dimension_scores


def calculate_column_risk_scores(report):
    """
    Calculate risk score per column across all dimensions.
    Risk = based on failure count and percentage scores.
    """
    column_data = defaultdict(lambda: {
        "dimensions_failed": [],
        "percentages": [],
        "failure_count": 0
    })
    
    for dimension, findings in report.items():
        for finding in findings:
            col = finding.get("column_name", "unknown")
            status = finding.get("status", "passed")
            percentage = finding.get("percentage", 100)
            
            # Track all percentages
            column_data[col]["percentages"].append(percentage)
            
            # Only consider failures
            if status == "failed":
                column_data[col]["dimensions_failed"].append(dimension)
                column_data[col]["failure_count"] += 1
    
    # Calculate risk scores
    column_risks = []
    
    for col, data in column_data.items():
        if not data["percentages"]:
            continue
            
        avg_percentage = mean(data["percentages"])
        failure_count = data["failure_count"]
        
        # Risk score: inverse of average percentage weighted by failure count
        # Higher failures and lower percentages = higher risk
        risk_score = (100 - avg_percentage) * (1 + failure_count * 0.2)
        risk_score = min(risk_score, 100)  # Cap at 100
        
        # Priority level based on risk score
        if risk_score >= 60:
            priority = "High"
        elif risk_score >= 30:
            priority = "Medium"
        else:
            priority = "Low"
        
        column_risks.append({
            "column_name": col,
            "risk_score": round(risk_score, 2),
            "dimensions_failed": list(set(data["dimensions_failed"])),
            "priority_level": priority,
            "failure_count": failure_count,
            "avg_percentage": round(avg_percentage, 2)
        })
    
    # Sort by risk score descending
    column_risks.sort(key=lambda x: x["risk_score"], reverse=True)
    return column_risks


def calculate_overall_quality_score(dimension_scores):
    """Calculate overall quality score as average of all dimensions."""
    if not dimension_scores:
        return 100
    return round(mean(dimension_scores.values()), 2)


def generate_priority_matrix(column_risks):
    """
    Generate priority matrix with impact and urgency scores.
    Impact = based on average percentage (lower = higher impact)
    Urgency = based on number of dimensions failed
    """
    priority_matrix = []
    
    for col_risk in column_risks:
        # Impact score: inverse of percentage (lower percentage = higher impact)
        impact_score = 100 - col_risk["avg_percentage"]
        
        # Urgency score: based on failure count
        urgency_score = min(col_risk["failure_count"] * 12.5, 100)
        
        priority_matrix.append({
            "column_name": col_risk["column_name"],
            "impact_score": round(impact_score, 2),
            "urgency_score": round(urgency_score, 2)
        })
    
    return priority_matrix


def main():
    """Main execution flow."""
    print("=" * 60)
    print("COMPRESSOR AGENT - Data Quality Analysis")
    print("=" * 60)
    
    # Load report
    report = load_report()
    
    # Calculate dimension scores
    dimension_scores = calculate_dimension_scores(report)
    
    # Calculate overall quality score
    overall_score = calculate_overall_quality_score(dimension_scores)
    
    # Calculate column risk scores
    column_risks = calculate_column_risk_scores(report)
    
    # Generate priority matrix
    priority_matrix = generate_priority_matrix(column_risks)
    
    # Filter high risk columns and merge with priority actions
    high_risk_columns = []
    for col_risk in column_risks:
        if col_risk["priority_level"] == "High":
            action_data = next((p for p in priority_matrix if p["column_name"] == col_risk["column_name"]), {})
            high_risk_columns.append({
                "column_name": col_risk["column_name"],
                "risk_score": col_risk["risk_score"],
                "dimensions_failed": col_risk["dimensions_failed"],
                "failure_count": col_risk["failure_count"],
                "avg_percentage": col_risk["avg_percentage"],
                "impact_score": action_data.get("impact_score", 0),
                "urgency_score": action_data.get("urgency_score", 0)
            })
    
    # Output structure
    print("\n1️⃣  OVERALL QUALITY SCORE")
    print("-" * 60)
    print(json.dumps({"overall_quality_score": overall_score}, indent=2))
    
    print("\n\n2️⃣  DIMENSION SCORES")
    print("-" * 60)
    print(json.dumps(dimension_scores, indent=2))
    
    print("\n\n3️⃣  CRITICAL COLUMNS & PRIORITY ACTIONS")
    print("-" * 60)
    print(json.dumps(high_risk_columns, indent=2))
    
    # Save all outputs to single file
    combined_output = {
        "overall_quality_score": overall_score,
        "dimension_scores": dimension_scores,
        "critical_columns_with_actions": high_risk_columns
    }
    
    with open("compressor_output.json", "w") as f:
        json.dump(combined_output, f, indent=2)
    
    print("\n" + "=" * 60)
    print("✅ Analysis complete. Output saved to compressor_output.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
