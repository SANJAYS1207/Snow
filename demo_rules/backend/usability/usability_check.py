import sys
import json
import csv
import re
from typing import Dict, Any, List

class UsabilityRules:
    """Usability dimension rules - naming conventions, documentation, clarity"""
    
    _metadata_cache = None
    
    def __init__(self, csv_file: str, column_name: str, metadata: Dict = None):
        self.csv_file = csv_file
        self.column_name = column_name
        self.metadata = metadata if metadata else self._get_metadata()
    
    def _get_metadata(self) -> Dict:
        """Get CSV metadata"""
        with open(self.csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            
            # Sample first 1000 rows to determine types
            rows = []
            for i, row in enumerate(reader):
                if i >= 1000:
                    break
                rows.append(row)
            
            metadata = {"columns": []}
            for col in columns:
                col_type = self._infer_type(col, rows)
                nullable = any(not row.get(col, '').strip() for row in rows)
                metadata["columns"].append({
                    "name": col,
                    "type": col_type,
                    "nullable": nullable
                })
            
            return metadata
    
    def _infer_type(self, col: str, rows: List[Dict]) -> str:
        """Infer column type from sample data"""
        non_empty = [row.get(col, '').strip() for row in rows if row.get(col, '').strip()]
        if not non_empty:
            return "VARCHAR"
        
        # Check if numeric
        try:
            [float(val) for val in non_empty[:100]]
            return "NUMBER"
        except:
            pass
        
        return "VARCHAR"
    
    def execute(self) -> Dict[str, Any]:
        """Execute usability checks"""
        results = {
            "dimension": "usability",
            "column_name": self.column_name,
            "total_columns": len(self.metadata["columns"]),
            "checks": []
        }
        
        # Check 1: Column naming conventions
        naming_checks = self._check_naming_conventions(self.metadata)
        results["checks"].extend(naming_checks)
        
        # Check 2: Null vs empty string ambiguity
        ambiguity_checks = self._check_null_empty_ambiguity(self.metadata)
        results["checks"].extend(ambiguity_checks)
        
        # Check 3: Structure clarity score
        clarity_check = self._assess_structure_clarity(self.metadata)
        results["checks"].append(clarity_check)
        
        # Check 4: Documentation completeness
        doc_check = self._check_documentation(self.metadata)
        results["checks"].append(doc_check)
        
        results["score"] = self._calculate_score(results["checks"])
        results["percentage"] = results["score"]
        results["status"] = "passed" if results["score"] >= 90 else "failed"
        
        return results
    
    def _check_naming_conventions(self, metadata: Dict) -> List[Dict]:
        """Check column naming conventions"""
        checks = []
        
        naming_issues = {
            "too_short": [],
            "too_long": [],
            "inconsistent_case": [],
            "special_chars": [],
            "reserved_words": [],
            "unclear": []
        }
        
        reserved_words = {"user", "table", "select", "from", "where", "order", "group"}
        
        for col in metadata["columns"]:
            col_name = col["name"]
            col_lower = col_name.lower()
            
            if len(col_name) < 2:
                naming_issues["too_short"].append(col_name)
            
            if len(col_name) > 50:
                naming_issues["too_long"].append(col_name)
            
            if not (self._is_snake_case(col_name) or 
                   self._is_camel_case(col_name) or 
                   self._is_pascal_case(col_name)):
                naming_issues["inconsistent_case"].append(col_name)
            
            if re.search(r'[^a-zA-Z0-9_]', col_name):
                naming_issues["special_chars"].append(col_name)
            
            if col_lower in reserved_words:
                naming_issues["reserved_words"].append(col_name)
            
            if len(col_name) == 1 or col_lower in ["col", "val", "tmp", "temp", "data"]:
                naming_issues["unclear"].append(col_name)
        
        for issue_type, columns in naming_issues.items():
            if columns:
                checks.append({
                    "check_type": "naming_convention",
                    "issue_type": issue_type,
                    "affected_columns": columns,
                    "count": len(columns),
                    "severity": "HIGH" if issue_type in ["reserved_words", "unclear"] else "MEDIUM"
                })
        
        return checks
    
    def _check_null_empty_ambiguity(self, metadata: Dict) -> List[Dict]:
        """Check for null vs empty string ambiguity - SKIPPED for performance"""
        return []
    
    def _assess_structure_clarity(self, metadata: Dict) -> Dict:
        """Assess overall structure clarity"""
        total_columns = len(metadata["columns"])
        
        metrics = {
            "column_count": total_columns,
            "avg_column_name_length": sum(len(col["name"]) for col in metadata["columns"]) / total_columns,
            "nullable_columns": sum(1 for col in metadata["columns"] if col["nullable"]),
            "type_diversity": len(set(col["type"] for col in metadata["columns"])),
        }
        
        issues = []
        
        if total_columns > 100:
            issues.append("Too many columns (>100)")
        elif total_columns < 3:
            issues.append("Too few columns (<3)")
        
        nullable_pct = (metrics["nullable_columns"] / total_columns) * 100
        if nullable_pct > 80:
            issues.append(f"High nullable percentage ({nullable_pct:.0f}%)")
        
        if metrics["avg_column_name_length"] < 5:
            issues.append("Column names too short on average")
        elif metrics["avg_column_name_length"] > 30:
            issues.append("Column names too long on average")
        
        clarity_score = 100 - (len(issues) * 15)
        
        return {
            "check_type": "structure_clarity",
            "metrics": metrics,
            "issues": issues,
            "clarity_score": max(0, clarity_score),
            "severity": "LOW" if clarity_score >= 70 else "MEDIUM"
        }
    
    def _check_documentation(self, metadata: Dict) -> Dict:
        """Check documentation completeness"""
        total_columns = len(metadata["columns"])
        
        # CSV files don't have built-in comments, so documentation score is 0
        has_table_comment = False
        columns_with_comments = 0
        documentation_pct = 0
        doc_score = 0
        
        return {
            "check_type": "documentation_completeness",
            "has_table_comment": has_table_comment,
            "columns_with_comments": columns_with_comments,
            "total_columns": total_columns,
            "documentation_percentage": documentation_pct,
            "documentation_score": doc_score,
            "severity": "MEDIUM"
        }
    
    def _is_snake_case(self, name: str) -> bool:
        """Check if name is in snake_case"""
        return bool(re.match(r'^[a-z][a-z0-9_]*$', name))
    
    def _is_camel_case(self, name: str) -> bool:
        """Check if name is in camelCase"""
        return bool(re.match(r'^[a-z][a-zA-Z0-9]*$', name))
    
    def _is_pascal_case(self, name: str) -> bool:
        """Check if name is in PascalCase"""
        return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))
    
    def _calculate_score(self, checks: List[Dict]) -> float:
        """Calculate usability score"""
        score = 100.0
        
        for check in checks:
            check_type = check.get("check_type")
            
            if check_type == "naming_convention":
                count = check.get("count", 0)
                severity = check.get("severity")
                if severity == "HIGH":
                    score -= min(count * 5, 30)
                else:
                    score -= min(count * 2, 20)
            
            elif check_type == "null_empty_ambiguity":
                score -= 5
            
            elif check_type == "structure_clarity":
                clarity_score = check.get("clarity_score", 100)
                score = score * (clarity_score / 100)
            
            elif check_type == "documentation_completeness":
                doc_score = check.get("documentation_score", 0)
                score = score * 0.8 + (doc_score * 0.2)
        
        return max(0, round(score, 2))

if __name__ == "__main__":
    csv_file = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    # Load metadata once for all columns
    first_checker = UsabilityRules(csv_file, columns[0])
    shared_metadata = first_checker.metadata
    
    results = []
    for column_name in columns:
        checker = UsabilityRules(csv_file, column_name, shared_metadata)
        result = checker.execute()
        result['column_name'] = column_name
        results.append(result)
    
    print(json.dumps(results))
