import sys
import json
import csv
import time
import os
from typing import Dict, List

class AvailabilityRules:
    """Availability dimension rules - CSV file access, read performance, data health"""
    
    def execute(self, csv_path: str, columns: List[str]) -> Dict:
        """Execute availability checks"""
        results = []
        
        for col in columns:
            col_result = {
                "column_name": col,
                "checks": []
            }
            
            # Check 1: Column accessibility
            access_check = self._check_column_accessibility(csv_path, col)
            col_result["checks"].append(access_check)
            
            if not access_check.get("accessible"):
                col_result["score"] = 0
                col_result["status"] = "failed"
                col_result["percentage"] = 0.0
                col_result["accessible"] = False
                results.append(col_result)
                continue
            
            # Check 2: Read performance
            performance_check = self._check_read_performance(csv_path, col)
            col_result["checks"].append(performance_check)
            
            # Check 3: File availability
            file_check = self._check_file_availability(csv_path)
            col_result["checks"].append(file_check)
            
            # Check 4: Data availability
            data_check = self._check_data_availability(csv_path, col)
            col_result["checks"].append(data_check)
            
            # Calculate score
            score = self._calculate_score(col_result["checks"])
            col_result["score"] = score
            col_result["status"] = "passed" if score >= 99 else "failed"
            col_result["percentage"] = score
            col_result["accessible"] = True
            col_result["accessible_rows"] = access_check.get("accessible_rows", 0)
            col_result["total_rows"] = access_check.get("total_rows", 0)
            col_result["response_time_ms"] = performance_check.get("response_time_ms", 0)
            
            results.append(col_result)
        
        return results
    
    def _check_column_accessibility(self, csv_path: str, col: str) -> Dict:
        """Check if column is accessible"""
        try:
            start_time = time.time()
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                if col not in headers:
                    return {
                        "check_type": "column_accessibility",
                        "accessible": False,
                        "error": "Column not found",
                        "severity": "HIGH"
                    }
                
                col_idx = headers.index(col)
                data_rows = list(reader)
                accessible_rows = sum(1 for row in data_rows if col_idx < len(row))
                
                response_time = time.time() - start_time
                
                return {
                    "check_type": "column_accessibility",
                    "accessible": True,
                    "response_time_seconds": round(response_time, 3),
                    "accessible_rows": accessible_rows,
                    "total_rows": len(data_rows),
                    "severity": "LOW"
                }
        
        except Exception as e:
            return {
                "check_type": "column_accessibility",
                "accessible": False,
                "error": str(e),
                "severity": "HIGH"
            }
    
    def _check_read_performance(self, csv_path: str, col: str) -> Dict:
        """Check read performance metrics"""
        
        # Test 1: Count rows
        start_time = time.time()
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                row_count = sum(1 for _ in reader)
            count_time = time.time() - start_time
        except:
            count_time = None
        
        # Test 2: Read column data
        start_time = time.time()
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                data = [row[col_idx] for row in reader if col_idx < len(row)]
            read_time = time.time() - start_time
        except:
            read_time = None
        
        # Assess performance
        performance_issues = []
        
        if count_time and count_time > 5:
            performance_issues.append("Row count slow (>5s)")
        
        if read_time and read_time > 10:
            performance_issues.append("Column read slow (>10s)")
        
        # Calculate performance score
        perf_score = 100
        if count_time and count_time > 5:
            perf_score -= 20
        if read_time and read_time > 10:
            perf_score -= 20
        
        return {
            "check_type": "read_performance",
            "count_time_seconds": round(count_time, 3) if count_time else None,
            "read_time_seconds": round(read_time, 3) if read_time else None,
            "response_time_ms": round(read_time * 1000, 2) if read_time else 0,
            "performance_score": perf_score,
            "issues": performance_issues,
            "severity": "HIGH" if performance_issues else "LOW"
        }
    
    def _check_file_availability(self, csv_path: str) -> Dict:
        """Check CSV file availability"""
        
        try:
            # Check file exists
            file_exists = os.path.exists(csv_path)
            
            if not file_exists:
                return {
                    "check_type": "file_availability",
                    "file_available": False,
                    "error": "File not found",
                    "severity": "HIGH"
                }
            
            # Check file size
            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            
            # Check file readable
            with open(csv_path, 'r', encoding='utf-8') as f:
                f.readline()
            
            return {
                "check_type": "file_availability",
                "file_available": True,
                "file_size_mb": round(file_size_mb, 2),
                "file_readable": True,
                "severity": "LOW"
            }
        
        except Exception as e:
            return {
                "check_type": "file_availability",
                "file_available": False,
                "error": str(e),
                "severity": "HIGH"
            }
    
    def _check_data_availability(self, csv_path: str, col: str) -> Dict:
        """Check data availability metrics"""
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                
                total_rows = 0
                available_rows = 0
                
                for row in reader:
                    total_rows += 1
                    if col_idx < len(row) and row[col_idx]:
                        available_rows += 1
                
                availability_pct = (available_rows / total_rows * 100) if total_rows > 0 else 0
                
                # Assess availability
                data_status = "EXCELLENT"
                if availability_pct < 99:
                    data_status = "GOOD"
                if availability_pct < 95:
                    data_status = "FAIR"
                if availability_pct < 90:
                    data_status = "POOR"
                
                return {
                    "check_type": "data_availability",
                    "total_rows": total_rows,
                    "available_rows": available_rows,
                    "availability_percentage": round(availability_pct, 2),
                    "data_status": data_status,
                    "severity": "LOW" if availability_pct >= 99 else "MEDIUM"
                }
        
        except Exception as e:
            return {
                "check_type": "data_availability",
                "error": str(e),
                "severity": "LOW"
            }
    
    def _calculate_score(self, checks: List[Dict]) -> float:
        """Calculate availability score"""
        score = 100.0
        
        for check in checks:
            check_type = check.get("check_type")
            
            if check_type == "column_accessibility":
                if not check.get("accessible"):
                    return 0
                response_time = check.get("response_time_seconds", 0)
                if response_time > 5:
                    score -= 10
            
            elif check_type == "read_performance":
                perf_score = check.get("performance_score", 100)
                score = score * 0.7 + perf_score * 0.3
            
            elif check_type == "file_availability":
                if not check.get("file_available"):
                    score -= 30
            
            elif check_type == "data_availability":
                avail_pct = check.get("availability_percentage", 100)
                if avail_pct < 99:
                    score -= (99 - avail_pct)
        
        return max(0, round(score, 2))

if __name__ == "__main__":
    csv_path = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    checker = AvailabilityRules()
    result = checker.execute(csv_path, columns)
    print(json.dumps(result, indent=2))
