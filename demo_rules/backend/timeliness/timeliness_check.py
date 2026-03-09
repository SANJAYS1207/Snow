import sys
import json
import csv
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict

class TimelinessRules:
    """Timeliness dimension rules - data freshness, stale records, update frequency"""
    
    def execute(self, csv_path: str, columns: List[str]) -> Dict:
        """Execute timeliness checks"""
        results = []
        
        for col in columns:
            col_result = {
                "column_name": col,
                "checks": []
            }
            
            # Detect if column is timestamp
            is_timestamp = self._is_timestamp_column(csv_path, col)
            
            if not is_timestamp:
                col_result["checks"].append({
                    "check_type": "no_timestamp_data",
                    "message": "Column does not contain timestamp data",
                    "severity": "HIGH"
                })
                col_result["score"] = 0
                col_result["status"] = "failed"
                col_result["percentage"] = 0.0
                results.append(col_result)
                continue
            
            # Check 1: Data freshness
            freshness_check = self._check_data_freshness(csv_path, col)
            col_result["checks"].append(freshness_check)
            
            # Check 2: Stale record detection
            stale_checks = self._detect_stale_records(csv_path, col)
            col_result["checks"].extend(stale_checks)
            
            # Check 3: Update frequency
            frequency_check = self._analyze_update_frequency(csv_path, col)
            col_result["checks"].append(frequency_check)
            
            # Check 4: SLA compliance
            sla_check = self._check_sla_compliance(csv_path, col)
            col_result["checks"].append(sla_check)
            
            # Calculate score
            score = self._calculate_score(col_result["checks"])
            col_result["score"] = score
            col_result["status"] = "passed" if score >= 90 else "failed"
            col_result["percentage"] = score
            
            results.append(col_result)
        
        return results
    
    def _is_timestamp_column(self, csv_path: str, col: str) -> bool:
        """Check if column contains timestamp data"""
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                
                # Check first 10 non-empty values
                count = 0
                valid_dates = 0
                
                for row in reader:
                    if col_idx < len(row) and row[col_idx]:
                        count += 1
                        try:
                            # Try parsing as date
                            datetime.strptime(row[col_idx], '%m/%d/%Y')
                            valid_dates += 1
                        except:
                            try:
                                datetime.strptime(row[col_idx], '%Y-%m-%d')
                                valid_dates += 1
                            except:
                                pass
                    
                    if count >= 10:
                        break
                
                return valid_dates >= 5  # At least 50% valid dates
        except:
            return False
    
    def _check_data_freshness(self, csv_path: str, col: str) -> Dict:
        """Check how fresh the data is"""
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                
                dates = []
                for row in reader:
                    if col_idx < len(row) and row[col_idx]:
                        try:
                            date = datetime.strptime(row[col_idx], '%m/%d/%Y')
                            dates.append(date)
                        except:
                            try:
                                date = datetime.strptime(row[col_idx], '%Y-%m-%d')
                                dates.append(date)
                            except:
                                pass
                
                if not dates:
                    return {
                        "check_type": "data_freshness",
                        "error": "No valid dates found",
                        "severity": "HIGH"
                    }
                
                last_update = max(dates)
                now = datetime.now()
                hours_since = (now - last_update).total_seconds() / 3600
                days_since = (now - last_update).days
                
                # Determine severity
                if hours_since < 24:
                    severity = "LOW"
                    freshness = "FRESH"
                elif hours_since < 72:
                    severity = "MEDIUM"
                    freshness = "RECENT"
                elif days_since < 30:
                    severity = "MEDIUM"
                    freshness = "STALE"
                else:
                    severity = "HIGH"
                    freshness = "VERY_STALE"
                
                return {
                    "check_type": "data_freshness",
                    "last_update": last_update.strftime('%Y-%m-%d'),
                    "hours_since_update": round(hours_since, 2),
                    "days_since_update": days_since,
                    "freshness": freshness,
                    "severity": severity
                }
        except Exception as e:
            return {
                "check_type": "data_freshness",
                "error": str(e),
                "severity": "HIGH"
            }
    
    def _detect_stale_records(self, csv_path: str, col: str) -> List[Dict]:
        """Detect stale records"""
        checks = []
        thresholds = [(30, "30_days"), (90, "90_days"), (365, "1_year")]
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                
                now = datetime.now()
                
                for days, label in thresholds:
                    stale_count = 0
                    threshold_date = now - timedelta(days=days)
                    
                    f.seek(0)
                    next(reader)
                    
                    for row in reader:
                        if col_idx < len(row) and row[col_idx]:
                            try:
                                date = datetime.strptime(row[col_idx], '%m/%d/%Y')
                                if date < threshold_date:
                                    stale_count += 1
                            except:
                                try:
                                    date = datetime.strptime(row[col_idx], '%Y-%m-%d')
                                    if date < threshold_date:
                                        stale_count += 1
                                except:
                                    pass
                    
                    if stale_count > 0:
                        checks.append({
                            "check_type": "stale_records",
                            "threshold": f"{days} days",
                            "stale_count": stale_count,
                            "severity": "LOW" if days >= 365 else "MEDIUM"
                        })
        except:
            pass
        
        return checks
    
    def _analyze_update_frequency(self, csv_path: str, col: str) -> Dict:
        """Analyze update frequency pattern"""
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                
                daily_counts = defaultdict(int)
                now = datetime.now()
                thirty_days_ago = now - timedelta(days=30)
                
                for row in reader:
                    if col_idx < len(row) and row[col_idx]:
                        try:
                            date = datetime.strptime(row[col_idx], '%m/%d/%Y')
                            if date >= thirty_days_ago:
                                daily_counts[date.date()] += 1
                        except:
                            try:
                                date = datetime.strptime(row[col_idx], '%Y-%m-%d')
                                if date >= thirty_days_ago:
                                    daily_counts[date.date()] += 1
                            except:
                                pass
                
                days_with_updates = len(daily_counts)
                
                # Determine pattern
                if days_with_updates >= 28:
                    pattern = "DAILY"
                    severity = "LOW"
                elif days_with_updates >= 20:
                    pattern = "FREQUENT"
                    severity = "LOW"
                elif days_with_updates >= 8:
                    pattern = "WEEKLY"
                    severity = "MEDIUM"
                else:
                    pattern = "INFREQUENT"
                    severity = "MEDIUM"
                
                return {
                    "check_type": "update_frequency",
                    "days_with_updates_last_30d": days_with_updates,
                    "pattern": pattern,
                    "severity": severity
                }
        except Exception as e:
            return {
                "check_type": "update_frequency",
                "error": str(e),
                "severity": "LOW"
            }
    
    def _check_sla_compliance(self, csv_path: str, col: str) -> Dict:
        """Check SLA compliance (24 hour freshness)"""
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                col_idx = headers.index(col)
                
                dates = []
                for row in reader:
                    if col_idx < len(row) and row[col_idx]:
                        try:
                            date = datetime.strptime(row[col_idx], '%m/%d/%Y')
                            dates.append(date)
                        except:
                            try:
                                date = datetime.strptime(row[col_idx], '%Y-%m-%d')
                                dates.append(date)
                            except:
                                pass
                
                if dates:
                    last_update = max(dates)
                    now = datetime.now()
                    hours_since = (now - last_update).total_seconds() / 3600
                    
                    status = "MET" if hours_since <= 24 else "VIOLATED"
                    
                    return {
                        "check_type": "sla_compliance",
                        "sla_requirement": "24 hour freshness",
                        "status": status,
                        "hours_since_update": round(hours_since, 2),
                        "severity": "HIGH" if status == "VIOLATED" else "LOW"
                    }
        except:
            pass
        
        return {
            "check_type": "sla_compliance",
            "status": "UNKNOWN",
            "severity": "LOW"
        }
    
    def _calculate_score(self, checks: List[Dict]) -> float:
        """Calculate timeliness score"""
        score = 100.0
        
        for check in checks:
            check_type = check.get("check_type")
            
            if check_type == "data_freshness":
                freshness = check.get("freshness")
                if freshness == "VERY_STALE":
                    score -= 40
                elif freshness == "STALE":
                    score -= 20
                elif freshness == "RECENT":
                    score -= 10
            
            elif check_type == "stale_records":
                score -= 5
            
            elif check_type == "update_frequency":
                pattern = check.get("pattern")
                if pattern == "INFREQUENT":
                    score -= 15
                elif pattern == "WEEKLY":
                    score -= 5
            
            elif check_type == "sla_compliance":
                if check.get("status") == "VIOLATED":
                    score -= 30
        
        return max(0, round(score, 2))

if __name__ == "__main__":
    csv_path = sys.argv[1]
    columns = json.loads(sys.argv[2])
    
    checker = TimelinessRules()
    result = checker.execute(csv_path, columns)
    print(json.dumps(result, indent=2))
