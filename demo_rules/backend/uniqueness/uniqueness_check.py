import sys
import json
import csv
from typing import Dict, Any, List
from collections import Counter

class UniquenessRules:
    """Uniqueness dimension rules"""
    
    def __init__(self, csv_file: str, column_name: str):
        self.csv_file = csv_file
        self.column_name = column_name
    
    def execute(self) -> Dict[str, Any]:
        """Execute uniqueness checks"""
        results = {
            "dimension": "uniqueness",
            "column_name": self.column_name,
            "checks": []
        }
        
        # Check 1: Primary key violations (duplicate detection)
        dup_check = self._check_primary_key_violations()
        results["checks"].append(dup_check)
        
        # Check 2: Near-duplicate detection
        near_dup_check = self._check_near_duplicates()
        if near_dup_check:
            results["checks"].append(near_dup_check)
        
        # Calculate uniqueness ratio
        results["uniqueness_ratio"] = dup_check["uniqueness_ratio"]
        results["unique_values"] = dup_check["unique_values"]
        results["total_values"] = dup_check["total_values"]
        results["duplicates"] = dup_check["duplicates"]
        results["percentage"] = dup_check["uniqueness_ratio"]
        
        results["score"] = self._calculate_score(results)
        results["status"] = "passed" if results["score"] >= 90 else "failed"
        
        return results
    
    def _check_primary_key_violations(self) -> Dict:
        """Check for duplicate values"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            values = []
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if val:
                    values.append(val)
            
            total = len(values)
            unique = len(set(values))
            duplicates = total - unique
            
            # Find duplicate groups
            value_counts = Counter(values)
            duplicate_groups = {val: count for val, count in value_counts.items() if count > 1}
            
            uniqueness_ratio = (unique / total * 100) if total > 0 else 0
            
            return {
                "check_type": "primary_key_violations",
                "key_columns": [self.column_name],
                "duplicate_groups": len(duplicate_groups),
                "total_duplicates": duplicates,
                "unique_values": unique,
                "total_values": total,
                "duplicates": duplicates,
                "uniqueness_ratio": round(uniqueness_ratio, 2),
                "severity": "HIGH" if duplicates > 0 else "LOW"
            }
    
    def _check_near_duplicates(self) -> Dict:
        """Check for near-duplicates (normalized values)"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Normalize and count
            normalized_counts = Counter()
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if val:
                    normalized = val.lower().strip()
                    normalized_counts[normalized] += 1
            
            # Find near-duplicates (normalized values appearing multiple times)
            near_dups = {val: count for val, count in normalized_counts.items() if count > 1}
            
            if near_dups:
                top_duplicate = max(near_dups.items(), key=lambda x: x[1])
                return {
                    "check_type": "near_duplicates",
                    "column": self.column_name,
                    "duplicate_values": len(near_dups),
                    "top_duplicate": {"value": top_duplicate[0], "count": top_duplicate[1]},
                    "severity": "MEDIUM"
                }
        
        return None
    
    def _calculate_score(self, results: Dict) -> float:
        """Calculate uniqueness score"""
        score = results.get("uniqueness_ratio", 100.0)
        
        # Penalize for violations
        for check in results["checks"]:
            if check["check_type"] == "primary_key_violations":
                score -= min(check.get("total_duplicates", 0) * 0.1, 30)
        
        return max(0, min(100, round(score, 2)))

if __name__ == "__main__":
    csv_file = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    results = []
    for column_name in columns:
        checker = UniquenessRules(csv_file, column_name)
        result = checker.execute()
        result['column_name'] = column_name
        results.append(result)
    
    print(json.dumps(results))
