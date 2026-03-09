import json
import csv
import subprocess
from datetime import datetime
from collections import Counter
import sys

sys.path.append('compressor')
from llm_explainer import generate_explanations_from_data

def read_csv_data(csv_path):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        data_rows = [row for row in reader]
    return headers, data_rows

def infer_dtype(values):
    """Infer data type from sample values"""
    non_empty = [v for v in values if v and str(v).strip()]
    if not non_empty:
        return 'empty'
    
    try:
        nums = [float(v) for v in non_empty]
        if all(float(v).is_integer() for v in non_empty):
            return 'int'
        return 'float'
    except:
        pass
    
    date_keywords = ['date', 'time', 'timestamp', '/', '-', ':']
    if any(kw in str(non_empty[0]).lower() for kw in date_keywords):
        return 'datetime'
    
    return 'object'

def analyze_column(col_name, values):
    """Analyze column name, dtype, and sample values"""
    col_lower = str(col_name).lower()
    dtype = infer_dtype(values)
    non_empty = [v for v in values if v and str(v).strip()]
    
    total = len(values)
    filled = len(non_empty)
    unique = len(set(non_empty))
    
    return {
        'name': col_name,
        'name_lower': col_lower,
        'dtype': dtype,
        'total': total,
        'filled': filled,
        'unique': unique,
        'fill_rate': filled/total if total > 0 else 0,
        'unique_rate': unique/filled if filled > 0 else 0,
        'samples': non_empty[:5]
    }

def apply_rules(col_info):
    """Apply data quality rules based on analysis"""
    rules = []
    
    name = col_info['name_lower']
    dtype = col_info['dtype']
    unique_rate = col_info['unique_rate']
    
    if any(kw in name for kw in ['id', 'name', 'email', 'phone', 'customer', 'product', 'code']):
        rules.append('Completeness')
    
    if any(kw in name for kw in ['id', 'key', 'uuid', 'number']) or unique_rate > 0.9:
        rules.append('Uniqueness')
    
    if dtype in ['int', 'float'] or any(kw in name for kw in ['price', 'amount', 'quantity', 'rate', 'email', 'phone']):
        rules.append('Accuracy')
    
    if dtype == 'object' and unique_rate < 0.1 or any(kw in name for kw in ['type', 'status', 'category', 'code']):
        rules.append('Consistency')
    
    if dtype == 'datetime' or any(kw in name for kw in ['date', 'time', 'created', 'updated', 'modified']):
        rules.append('Timeliness')
    
    if any(kw in name for kw in ['email', 'phone', 'url', 'zip', 'postal', 'code']):
        rules.append('Validity')
    
    if any(kw in name for kw in ['name', 'title', 'description', 'text', 'label']):
        rules.append('Usability')
    
    if any(kw in name for kw in ['status', 'active', 'available', 'enabled', 'flag']):
        rules.append('Availability')
    
    return list(set(rules))

def analyze_csv_columns_for_rules(csv_path):
    """Main analysis function"""
    headers, data_rows = read_csv_data(csv_path)
    
    print("Column Names:", headers[:5], "... (total:", len(headers), ")")
    print("\nAnalyzing", len(data_rows), "sample rows...\n")
    
    columns_data = []
    for i, col_name in enumerate(headers):
        values = [row[i] if i < len(row) else '' for row in data_rows]
        columns_data.append((col_name, values))
    
    column_analyses = []
    for col_name, values in columns_data:
        col_info = analyze_column(col_name, values)
        column_analyses.append(col_info)
        print(f"[OK] {col_name}: dtype={col_info['dtype']}, filled={col_info['filled']}/{col_info['total']}, unique={col_info['unique']}")
    
    print("\n" + "="*50)
    print("Step 4: Applying Rules...\n")
    
    rule_mapping = {}
    for col_info in column_analyses:
        rules = apply_rules(col_info)
        for rule in rules:
            if rule not in rule_mapping:
                rule_mapping[rule] = []
            rule_mapping[rule].append(col_info['name'])
    
    result = [{"ruleName": rule, "columns": cols} for rule, cols in rule_mapping.items()]
    return result

def main(csv_file=None):
    if csv_file is None:
        csv_file = r"C:\Users\sanjay.s\Desktop\SnowSpeed\demo\demo_rules\Untitled_2026-02-27-1008(in).csv"
    
    print("Step 1: Analyzing columns and generating rule mapping...")
    result = analyze_csv_columns_for_rules(csv_file)
    
    with open('rule_mapping.json', 'w') as f:
        json.dump(result, f, indent=2)
    print("[OK] Rule mapping saved\n")
    
    column_results = {}
    
    rule_files = {
        'Completeness': 'completeness/completeness_check.py',
        'Uniqueness': 'uniqueness/uniqueness_check.py',
        'Accuracy': 'accuracy/accuracy_check.py',
        'Availability': 'availability/availability_check.py',
        'Timeliness': 'timeliness/timeliness_check.py',
        'Usability': 'usability/usability_check.py',
        'Validity': 'validity/validity_check.py',
        'Consistency': 'consistency/consistency_check.py'
    }
    
    for rule_entry in result:
        rule_name = rule_entry['ruleName']
        columns = rule_entry['columns']
        
        if rule_name not in rule_files:
            print(f"---->>> {rule_name}: No checker implemented yet <<<----\n")
            continue
        
        print(f"Running {rule_name} check...")
        
        cmd = ['python', rule_files[rule_name], csv_file, json.dumps(columns)]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            print(f"[ERROR] {rule_name} check timed out after 300 seconds\n")
            continue
        
        if proc.returncode == 0:
            rule_result = json.loads(proc.stdout)
            
            for item in rule_result:
                col_name = item['column_name']
                if col_name not in column_results:
                    column_results[col_name] = {}
                
                if rule_name == 'Completeness':
                    perc = item.get('percentage', 0)
                    column_results[col_name]['completeness'] = {
                        'filled_rows': item['filled_rows'],
                        'total_rows': item['total_rows'],
                        'null_rows': item.get('null_rows', 0),
                        'percentage': perc,
                        'threshold': 90,
                        'status': item['status'],
                        'score': item.get('score', perc),
                        'checks': item.get('checks', [])
                    }
                elif rule_name == 'Uniqueness':
                    perc = item.get('percentage', 0)
                    column_results[col_name]['uniqueness'] = {
                        'unique_values': item['unique_values'],
                        'total_values': item['total_values'],
                        'duplicates': item['duplicates'],
                        'percentage': perc,
                        'threshold': 95,
                        'status': item['status'],
                        'score': item.get('score', perc),
                        'checks': item.get('checks', [])
                    }
                elif rule_name == 'Accuracy':
                    perc = item.get('percentage', 0)
                    column_results[col_name]['accuracy'] = {
                        'valid_values': item['valid_values'],
                        'invalid_values': item['invalid_values'],
                        'total_values': item['total_values'],
                        'percentage': perc,
                        'status': item['status'],
                        'score': item.get('score', perc),
                        'checks': item.get('checks', [])
                    }
                elif rule_name == 'Availability':
                    perc = item['percentage']
                    column_results[col_name]['availability'] = {
                        'accessible': item['accessible'],
                        'accessible_rows': item.get('accessible_rows', 0),
                        'total_rows': item.get('total_rows', 0),
                        'percentage': perc,
                        'response_time_ms': item.get('response_time_ms', 0),
                        'status': item['status']
                    }
                elif rule_name == 'Timeliness':
                    perc = item['percentage']
                    column_results[col_name]['timeliness'] = {
                        'score': item['score'],
                        'percentage': perc,
                        'status': item['status'],
                        'checks': item.get('checks', [])
                    }
                elif rule_name == 'Usability':
                    perc = item['percentage']
                    column_results[col_name]['usability'] = {
                        'score': item['score'],
                        'percentage': perc,
                        'status': item['status'],
                        'checks': item.get('checks', [])
                    }
                elif rule_name == 'Validity':
                    perc = item['percentage']
                    column_results[col_name]['validity'] = {
                        'score': item['score'],
                        'percentage': perc,
                        'status': item['status'],
                        'checks': item.get('checks', [])
                    }
                elif rule_name == 'Consistency':
                    perc = item['percentage']
                    column_results[col_name]['consistency'] = {
                        'score': item['score'],
                        'percentage': perc,
                        'status': item['status'],
                        'checks': item.get('checks', [])
                    }
            
            result_path = f'{rule_name.lower()}/{rule_name.lower()}_result.json'
            with open(result_path, 'w') as f:
                json.dump(rule_result, f, indent=2)
            print(f"[OK] {rule_name} completed\n")
        else:
            print(f"[ERROR] Error in {rule_name}: {proc.stderr}\n")
    
    metric_report = {
        "Completeness": [],
        "Validity": [],
        "Consistency": [],
        "Accuracy": [],
        "Uniqueness": [],
        "Timeliness": [],
        "Availability": [],
        "Usability": []
    }
    
    for column_name, metrics in column_results.items():
        if 'completeness' in metrics:
            c = metrics['completeness']
            metric_report["Completeness"].append({
                "column_name": column_name,
                "filled_rows": c.get('filled_rows', 0),
                "total_rows": c.get('total_rows', 0),
                "null_rows": c.get('null_rows', 0),
                "percentage": c.get('percentage', 0),
                "threshold": c.get('threshold', 90),
                "status": c.get('status', 'failed')
            })
        
        if 'uniqueness' in metrics:
            u = metrics['uniqueness']
            metric_report["Uniqueness"].append({
                "column_name": column_name,
                "unique_values": u.get('unique_values', 0),
                "total_values": u.get('total_values', 0),
                "duplicates": u.get('duplicates', 0),
                "percentage": u.get('percentage', 0),
                "threshold": u.get('threshold', 95),
                "status": u.get('status', 'failed')
            })
        
        if 'accuracy' in metrics:
            a = metrics['accuracy']
            metric_report["Accuracy"].append({
                "column_name": column_name,
                "valid_values": a.get('valid_values', 0),
                "invalid_values": a.get('invalid_values', 0),
                "total_values": a.get('total_values', 0),
                "percentage": a.get('percentage', 0),
                "threshold": 95,
                "status": a.get('status', 'failed')
            })
        
        if 'validity' in metrics:
            v = metrics['validity']
            metric_report["Validity"].append({
                "column_name": column_name,
                "score": v.get('score', 0),
                "percentage": v.get('percentage', 0),
                "threshold": 90,
                "status": v.get('status', 'failed')
            })
        
        if 'consistency' in metrics:
            c = metrics['consistency']
            metric_report["Consistency"].append({
                "column_name": column_name,
                "score": c.get('score', 0),
                "percentage": c.get('percentage', 0),
                "threshold": 90,
                "status": c.get('status', 'failed')
            })
        
        if 'timeliness' in metrics:
            t = metrics['timeliness']
            metric_report["Timeliness"].append({
                "column_name": column_name,
                "score": t.get('score', 0),
                "percentage": t.get('percentage', 0),
                "threshold": 90,
                "status": t.get('status', 'failed')
            })
        
        if 'availability' in metrics:
            a = metrics['availability']
            metric_report["Availability"].append({
                "column_name": column_name,
                "accessible": a.get('accessible', False),
                "percentage": a.get('percentage', 0),
                "threshold": 99,
                "status": a.get('status', 'failed')
            })
        
        if 'usability' in metrics:
            u = metrics['usability']
            metric_report["Usability"].append({
                "column_name": column_name,
                "score": u.get('score', 0),
                "percentage": u.get('percentage', 0),
                "threshold": 90,
                "status": u.get('status', 'failed')
            })
    
    print("\n[OK] Generating LLM explanations...")
    generate_explanations_from_data(metric_report)
    try:
        result = subprocess.run(
            ["python", "selfhealing/run_self_healing.py"], 
            cwd=".",
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.returncode == 0:
            print("[OK] Self-healing completed successfully")
        else:
            print(f"[ERROR] Self-healing failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("[ERROR] Self-healing timed out after 300 seconds")
    except Exception as e:
        print(f"[ERROR] Self-healing error: {str(e)}")

if __name__ == "__main__":
    import sys
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    main(csv_file)
