import sys
import json
import csv
import re
from typing import Dict, Any, List

class AccuracyRules:
    """Accuracy dimension rules - business rules, format compliance, outliers"""
    
    def __init__(self, csv_file: str, column_name: str):
        self.csv_file = csv_file
        self.column_name = column_name
    
    def execute(self) -> Dict[str, Any]:
        """Execute accuracy checks"""
        results = {
            "dimension": "accuracy",
            "column_name": self.column_name,
            "checks": []
        }
        
        # Check 1: Business rule violations
        business_check = self._check_business_rule()
        if business_check:
            results["checks"].append(business_check)
        
        # Check 2: Format compliance
        format_check = self._check_format_compliance()
        if format_check:
            results["checks"].append(format_check)
        
        # Check 3: Statistical outliers (sample only)
        outlier_check = self._detect_outliers()
        if outlier_check:
            results["checks"].append(outlier_check)
        
        # Check 4: Range validation
        range_check = self._check_range_validity()
        if range_check:
            results["checks"].append(range_check)
        
        # Calculate basic metrics
        valid, invalid, total = self._count_valid_values()
        results["valid_values"] = valid
        results["invalid_values"] = invalid
        results["total_values"] = total
        results["percentage"] = (valid / total * 100) if total > 0 else 0
        
        results["score"] = self._calculate_score(results["checks"], total)
        results["status"] = "passed" if results["score"] >= 90 else "failed"
        
        return results
    
    def _count_valid_values(self) -> tuple:
        """Count valid numeric values"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            valid = 0
            invalid = 0
            
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if not val:
                    continue
                
                try:
                    float(val)
                    valid += 1
                except:
                    invalid += 1
            
            return valid, invalid, valid + invalid
    
    def _check_business_rule(self) -> Dict:
        """Check business rule violations"""
        col_lower = self.column_name.lower()
        
        # Quantity/count must be positive
        if any(kw in col_lower for kw in ['quantity', 'count', 'qty']):
            violations = 0
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if val:
                        try:
                            if float(val) <= 0:
                                violations += 1
                        except:
                            pass
            
            if violations > 0:
                return {
                    "check_type": "business_rule_violation",
                    "rule": f"{self.column_name} > 0",
                    "column": self.column_name,
                    "violations": violations,
                    "severity": "HIGH"
                }
        
        return None
    
    def _check_format_compliance(self) -> Dict:
        """Check format compliance"""
        col_lower = self.column_name.lower()
        
        # Email validation
        if 'email' in col_lower:
            pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            invalid = 0
            
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if val and not re.match(pattern, val):
                        invalid += 1
            
            if invalid > 0:
                return {
                    "check_type": "format_compliance",
                    "column": self.column_name,
                    "format": "email",
                    "invalid_count": invalid,
                    "severity": "MEDIUM"
                }
        
        # Phone validation
        if 'phone' in col_lower:
            pattern = r'^\+?1?\d{10,14}$'
            invalid = 0
            
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if val and not re.match(pattern, val):
                        invalid += 1
            
            if invalid > 0:
                return {
                    "check_type": "format_compliance",
                    "column": self.column_name,
                    "format": "phone",
                    "invalid_count": invalid,
                    "severity": "MEDIUM"
                }
        
        return None
    
    def _detect_outliers(self) -> Dict:
        """Detect statistical outliers (sample first 1000 rows)"""
        col_lower = self.column_name.lower()
        
        # Only for numeric columns
        if not any(kw in col_lower for kw in ['amount', 'price', 'qty', 'quantity', 'rate']):
            return None
        
        values = []
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 1000:
                    break
                val = row.get(self.column_name, '').strip()
                if val:
                    try:
                        values.append(float(val))
                    except:
                        pass
        
        if len(values) < 10:
            return None
        
        # IQR method
        values.sort()
        q1_idx = len(values) // 4
        q3_idx = 3 * len(values) // 4
        q1 = values[q1_idx]
        q3 = values[q3_idx]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        
        outliers = sum(1 for v in values if v < lower or v > upper)
        
        if outliers > 0:
            return {
                "check_type": "statistical_outliers",
                "column": self.column_name,
                "outlier_count": outliers,
                "lower_bound": round(lower, 2),
                "upper_bound": round(upper, 2),
                "severity": "MEDIUM"
            }
        
        return None
    
    def _check_range_validity(self) -> Dict:
        """Check range validity"""
        col_lower = self.column_name.lower()
        
        # Age validation
        if 'age' in col_lower:
            violations = 0
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if val:
                        try:
                            age = float(val)
                            if age < 0 or age > 120:
                                violations += 1
                        except:
                            pass
            
            if violations > 0:
                return {
                    "check_type": "range_validation",
                    "column": self.column_name,
                    "rule": "age between 0 and 120",
                    "violations": violations,
                    "severity": "HIGH"
                }
        
        # Percentage validation
        if any(kw in col_lower for kw in ['percent', 'pct', 'rate']):
            violations = 0
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if val:
                        try:
                            pct = float(val)
                            if pct < 0 or pct > 100:
                                violations += 1
                        except:
                            pass
            
            if violations > 0:
                return {
                    "check_type": "range_validation",
                    "column": self.column_name,
                    "rule": "percentage between 0 and 100",
                    "violations": violations,
                    "severity": "HIGH"
                }
        
        # Amount/price validation (negative check)
        if any(kw in col_lower for kw in ['amount', 'price', 'cost', 'total']):
            violations = 0
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if val:
                        try:
                            if float(val) < 0:
                                violations += 1
                        except:
                            pass
            
            if violations > 0:
                return {
                    "check_type": "range_validation",
                    "column": self.column_name,
                    "rule": "amount >= 0",
                    "violations": violations,
                    "severity": "HIGH"
                }
        
        return None
    
    def _calculate_score(self, checks: List[Dict], total_rows: int) -> float:
        """Calculate accuracy score"""
        if not checks or total_rows == 0:
            return 100.0
        
        total_violations = sum(
            check.get("violations", 0) + check.get("invalid_count", 0) + 
            check.get("outlier_count", 0)
            for check in checks
        )
        
        violation_rate = (total_violations / total_rows) * 100
        score = max(0, 100 - violation_rate)
        
        return round(score, 2)

if __name__ == "__main__":
    csv_file = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    results = []
    for column_name in columns:
        checker = AccuracyRules(csv_file, column_name)
        result = checker.execute()
        result['column_name'] = column_name
        results.append(result)
    
    print(json.dumps(results))
