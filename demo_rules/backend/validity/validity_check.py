import sys
import json
import csv
import re
from typing import Dict, Any, List
from datetime import datetime

class ValidityRules:
    """Validity dimension rules - data types, format patterns, constraint violations"""
    
    def __init__(self, csv_file: str, column_name: str):
        self.csv_file = csv_file
        self.column_name = column_name
    
    def execute(self) -> Dict[str, Any]:
        """Execute validity checks"""
        results = {
            "dimension": "validity",
            "column_name": self.column_name,
            "checks": []
        }
        
        # Check 1: Data type enforcement
        type_check = self._check_data_type_validity()
        if type_check:
            results["checks"].append(type_check)
        
        # Check 2: Format pattern validation
        format_check = self._check_format_patterns()
        if format_check:
            results["checks"].append(format_check)
        
        # Check 3: Domain value compliance
        domain_check = self._check_domain_values()
        if domain_check:
            results["checks"].append(domain_check)
        
        # Check 4: Constraint violations
        constraint_check = self._check_constraints()
        if constraint_check:
            results["checks"].append(constraint_check)
        
        results["score"] = self._calculate_score(results["checks"])
        results["percentage"] = results["score"]
        results["status"] = "passed" if results["score"] >= 90 else "failed"
        
        return results
    
    def _check_data_type_validity(self) -> Dict:
        """Check if data conforms to expected types"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            total = 0
            invalid = 0
            
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if not val:
                    continue
                
                total += 1
                
                # Try to infer expected type and validate
                if self._looks_like_number(self.column_name):
                    try:
                        float(val)
                    except:
                        invalid += 1
                elif self._looks_like_date(self.column_name):
                    if not self._is_valid_date(val):
                        invalid += 1
        
        if invalid > 0:
            return {
                "check_type": "data_type_enforcement",
                "column": self.column_name,
                "invalid_count": invalid,
                "total_count": total,
                "severity": "HIGH"
            }
        return None
    
    def _check_format_patterns(self) -> Dict:
        """Check format pattern compliance using regex"""
        pattern = self._get_default_pattern(self.column_name)
        
        if not pattern:
            return None
        
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            invalid = 0
            total = 0
            
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if not val:
                    continue
                
                total += 1
                if not re.match(pattern, val, re.IGNORECASE):
                    invalid += 1
        
        return {
            "check_type": "format_pattern",
            "column": self.column_name,
            "pattern": pattern,
            "invalid_count": invalid,
            "total_count": total,
            "severity": "MEDIUM" if invalid > 0 else "LOW"
        }
    
    def _check_domain_values(self) -> Dict:
        """Check if values belong to expected domain"""
        domain_mappings = {
            "status": ["active", "inactive", "pending", "completed", "cancelled"],
            "type": ["type_a", "type_b", "type_c"],
            "category": ["cat1", "cat2", "cat3"],
            "gender": ["m", "f", "male", "female", "other", "unknown"],
        }
        
        col_lower = self.column_name.lower()
        expected_values = None
        domain_key = None
        
        for key, values in domain_mappings.items():
            if key in col_lower:
                expected_values = [v.lower() for v in values]
                domain_key = key
                break
        
        if not expected_values:
            return None
        
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            invalid = 0
            total = 0
            
            for row in reader:
                val = row.get(self.column_name, '').strip()
                if not val:
                    continue
                
                total += 1
                if val.lower() not in expected_values:
                    invalid += 1
        
        if invalid > 0:
            return {
                "check_type": "domain_value_compliance",
                "column": self.column_name,
                "expected_domain": domain_key,
                "invalid_count": invalid,
                "total_count": total,
                "severity": "MEDIUM"
            }
        return None
    
    def _check_constraints(self) -> Dict:
        """Check constraint violations"""
        col_lower = self.column_name.lower()
        
        # Check for negative values in columns that should be positive
        if any(kw in col_lower for kw in ["count", "quantity", "size", "length", "amount", "price"]):
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                violations = 0
                total = 0
                
                for row in reader:
                    val = row.get(self.column_name, '').strip()
                    if not val:
                        continue
                    
                    try:
                        num = float(val)
                        total += 1
                        if num < 0:
                            violations += 1
                    except:
                        pass
                
                if violations > 0:
                    return {
                        "check_type": "constraint_violation",
                        "constraint_type": "POSITIVE_VALUE",
                        "column": self.column_name,
                        "violations": violations,
                        "total_count": total,
                        "severity": "MEDIUM"
                    }
        
        return None
    
    def _looks_like_number(self, col_name: str) -> bool:
        """Check if column name suggests numeric data"""
        keywords = ["amount", "price", "quantity", "count", "rate", "number", "id"]
        return any(kw in col_name.lower() for kw in keywords)
    
    def _looks_like_date(self, col_name: str) -> bool:
        """Check if column name suggests date data"""
        keywords = ["date", "time", "timestamp", "created", "updated"]
        return any(kw in col_name.lower() for kw in keywords)
    
    def _is_valid_date(self, val: str) -> bool:
        """Check if value is a valid date"""
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']
        for fmt in formats:
            try:
                datetime.strptime(val, fmt)
                return True
            except:
                pass
        return False
    
    def _get_default_pattern(self, column_name: str) -> str:
        """Get default regex pattern based on column name"""
        column_lower = column_name.lower()
        
        patterns = {
            "email": r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',
            "phone": r'^\+?1?\d{10,14}$',
            "zip": r'^\d{5}(-\d{4})?$',
            "postal": r'^\d{5}(-\d{4})?$',
            "ssn": r'^\d{3}-\d{2}-\d{4}$',
            "url": r'^https?://[^\s/$.?#].[^\s]*$',
            "ip": r'^(\d{1,3}\.){3}\d{1,3}$',
            "uuid": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        }
        
        for key, pattern in patterns.items():
            if key in column_lower:
                return pattern
        
        return None
    
    def _calculate_score(self, checks: List[Dict]) -> float:
        """Calculate validity score"""
        if not checks:
            return 100.0
        
        total_violations = 0
        total_records = 0
        
        for check in checks:
            total_violations += check.get("invalid_count", 0) + check.get("violations", 0)
            total_records += check.get("total_count", 0)
        
        if total_records == 0:
            return 100.0
        
        violation_rate = (total_violations / total_records) * 100
        score = max(0, 100 - violation_rate)
        
        return round(score, 2)

if __name__ == "__main__":
    csv_file = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    results = []
    for column_name in columns:
        checker = ValidityRules(csv_file, column_name)
        result = checker.execute()
        result['column_name'] = column_name
        results.append(result)
    
    print(json.dumps(results))
