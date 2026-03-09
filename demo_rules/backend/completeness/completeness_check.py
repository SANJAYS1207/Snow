import sys
import json
import csv
from typing import Dict, Any, List

class CompletenessRules:
    """Completeness dimension rules"""
    
    def __init__(self, csv_file: str, column_name: str):
        self.csv_file = csv_file
        self.column_name = column_name
    
    def execute(self) -> Dict[str, Any]:
        """Execute completeness checks"""
        results = {
            "dimension": "completeness",
            "column_name": self.column_name,
            "checks": []
        }
        
        # Check 1: Null percentage
        null_check = self._check_null_percentage()
        results["checks"].append(null_check)
        
        # Check 2: Required field coverage
        required_check = self._check_required_field_coverage()
        if required_check:
            results["checks"].append(required_check)
        
        # Store basic metrics
        results["filled_rows"] = null_check["non_null_rows"]
        results["total_rows"] = null_check["total_rows"]
        results["null_rows"] = null_check["null_rows"]
        results["null_percentage"] = null_check["null_percentage"]
        results["percentage"] = 100 - null_check["null_percentage"]
        
        results["score"] = self._calculate_score(results["checks"])
        results["status"] = "passed" if results["score"] >= 90 else "failed"
        
        return results
    
    def _check_null_percentage(self) -> Dict:
        """Check null percentage for column"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            total_rows = 0
            non_null_rows = 0
            
            for row in reader:
                total_rows += 1
                val = row.get(self.column_name, '').strip()
                if val:
                    non_null_rows += 1
            
            null_rows = total_rows - non_null_rows
            null_percentage = (null_rows / total_rows * 100) if total_rows > 0 else 0
            
            severity = "HIGH" if null_percentage > 10 else "MEDIUM" if null_percentage > 5 else "LOW"
            
            return {
                "check_type": "null_percentage",
                "column": self.column_name,
                "total_rows": total_rows,
                "non_null_rows": non_null_rows,
                "null_rows": null_rows,
                "null_percentage": round(null_percentage, 2),
                "severity": severity
            }
    
    def _check_required_field_coverage(self) -> Dict:
        """Check if required field has violations (nulls)"""
        # Assume columns with 'id', 'key', 'code' in name are required
        col_lower = self.column_name.lower()
        is_required = any(kw in col_lower for kw in ['id', 'key', 'code', 'number'])
        
        if not is_required:
            return None
        
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            violations = 0
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if not val:
                    violations += 1
            
            if violations > 0:
                return {
                    "check_type": "required_field_coverage",
                    "column": self.column_name,
                    "violations": violations,
                    "severity": "HIGH"
                }
        
        return None
    
    def _calculate_score(self, checks: List[Dict]) -> float:
        """Calculate completeness score (0-100)"""
        if not checks:
            return 100.0
        
        total_penalty = 0
        for check in checks:
            if check["check_type"] == "null_percentage":
                total_penalty += check.get("null_percentage", 0) * 0.5
            elif check["check_type"] == "required_field_coverage":
                total_penalty += 10 if check.get("violations", 0) > 0 else 0
        
        score = max(0, 100 - (total_penalty / len(checks)))
        return round(score, 2)

if __name__ == "__main__":
    csv_file = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    results = []
    for column_name in columns:
        checker = CompletenessRules(csv_file, column_name)
        result = checker.execute()
        result['column_name'] = column_name
        results.append(result)
    
    print(json.dumps(results))
