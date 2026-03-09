import sys
import json
import csv
from typing import Dict, Any, List
from datetime import datetime

class ConsistencyRules:
    """Consistency dimension rules - referential integrity, cross-field rules"""
    
    def __init__(self, csv_file: str, column_name: str):
        self.csv_file = csv_file
        self.column_name = column_name
    
    def execute(self) -> Dict[str, Any]:
        """Execute consistency checks"""
        results = {
            "dimension": "consistency",
            "column_name": self.column_name,
            "checks": []
        }
        
        # Check 1: Referential integrity (skipped for single CSV)
        
        # Check 2: Cross-field consistency rules
        cross_field_checks = self._check_cross_field_rules()
        results["checks"].extend(cross_field_checks)
        
        # Check 3: Column relationship validation
        relationship_checks = self._check_column_relationships()
        results["checks"].extend(relationship_checks)
        
        # Check 4: Join consistency (skipped for single CSV)
        
        results["score"] = self._calculate_score(results["checks"])
        results["percentage"] = results["score"]
        results["status"] = "passed" if results["score"] >= 90 else "failed"
        
        return results
    
    def _check_cross_field_rules(self) -> List[Dict]:
        """Check cross-field consistency rules"""
        checks = []
        col_lower = self.column_name.lower()
        
        # Get all column names
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_columns = [c.lower() for c in reader.fieldnames]
        
        # Date range consistency (start_date < end_date)
        if 'start' in col_lower and 'date' in col_lower:
            end_col = self._find_matching_column(all_columns, col_lower, 'start', 'end')
            if end_col:
                violations = self._check_date_range(self.column_name, end_col)
                if violations > 0:
                    checks.append({
                        "check_type": "cross_field_rule",
                        "rule": f"{self.column_name} <= {end_col}",
                        "violations": violations,
                        "severity": "HIGH"
                    })
        
        # Min/Max consistency
        if 'min' in col_lower:
            max_col = self._find_matching_column(all_columns, col_lower, 'min', 'max')
            if max_col:
                violations = self._check_min_max(self.column_name, max_col)
                if violations > 0:
                    checks.append({
                        "check_type": "cross_field_rule",
                        "rule": f"{self.column_name} <= {max_col}",
                        "violations": violations,
                        "severity": "HIGH"
                    })
        
        return checks
    
    def _check_column_relationships(self) -> List[Dict]:
        """Check expected relationships between columns"""
        checks = []
        
        # Check for case inconsistency (sample first 1000 rows only)
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            values = []
            for i, row in enumerate(reader):
                if i >= 1000:
                    break
                val = row.get(self.column_name, '').strip()
                if val:
                    values.append(val)
            
            if values:
                unique_values = set(values)
                unique_lower = set(v.lower() for v in values)
                
                if len(unique_values) > len(unique_lower):
                    case_issues = len(unique_values) - len(unique_lower)
                    checks.append({
                        "check_type": "column_relationship",
                        "relationship": "case_consistency",
                        "inconsistent_values": case_issues,
                        "severity": "MEDIUM"
                    })
        
        return checks
    
    def _find_matching_column(self, all_columns: List[str], current_col: str, old_word: str, new_word: str) -> str:
        """Find matching column by replacing word"""
        expected = current_col.replace(old_word, new_word)
        
        # Try exact match
        for col in all_columns:
            if col == expected:
                # Return original case column name
                with open(self.csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for orig_col in reader.fieldnames:
                        if orig_col.lower() == expected:
                            return orig_col
        return None
    
    def _check_date_range(self, start_col: str, end_col: str) -> int:
        """Check if start_date <= end_date"""
        violations = 0
        
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                start_val = row.get(start_col, '').strip()
                end_val = row.get(end_col, '').strip()
                
                if not start_val or not end_val:
                    continue
                
                start_date = self._parse_date(start_val)
                end_date = self._parse_date(end_val)
                
                if start_date and end_date and start_date > end_date:
                    violations += 1
        
        return violations
    
    def _check_min_max(self, min_col: str, max_col: str) -> int:
        """Check if min_value <= max_value"""
        violations = 0
        
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                min_val = row.get(min_col, '').strip()
                max_val = row.get(max_col, '').strip()
                
                if not min_val or not max_val:
                    continue
                
                try:
                    min_num = float(min_val)
                    max_num = float(max_val)
                    
                    if min_num > max_num:
                        violations += 1
                except:
                    pass
        
        return violations
    
    def _parse_date(self, val: str) -> datetime:
        """Parse date string"""
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
        for fmt in formats:
            try:
                return datetime.strptime(val, fmt)
            except:
                pass
        return None
    
    def _calculate_score(self, checks: List[Dict]) -> float:
        """Calculate consistency score"""
        if not checks:
            return 100.0
        
        total_violations = sum(
            check.get("violations", 0) + check.get("inconsistent_values", 0)
            for check in checks
        )
        
        # Penalty based on violations
        score = 100.0 - min(total_violations * 0.1, 100)
        
        return max(0, round(score, 2))

if __name__ == "__main__":
    csv_file = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    results = []
    for column_name in columns:
        checker = ConsistencyRules(csv_file, column_name)
        result = checker.execute()
        result['column_name'] = column_name
        results.append(result)
    
    print(json.dumps(results))
