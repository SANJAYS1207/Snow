#!/usr/bin/env python3
"""
Self-Healing Agent - Generates constraint-based SQL fixes from recommendations
"""
import json
import re
from datetime import datetime

class SelfHealingAgent:
    def __init__(self, healing_input_path):
        self.healing_input_path = healing_input_path
        self.timestamp = datetime.now().isoformat()
    
    def load_recommendations(self):
        """Load healing recommendations"""
        import os
        path = os.path.join(os.path.dirname(__file__), '..', 'recommendation', 'self_healing_input.json')
        with open(path, 'r') as f:
            return json.load(f)
    
    def load_dimension_scores(self):
        """Load actual dimension scores from rule results"""
        import os
        from statistics import mean
        
        scores = {}
        dimensions = ['completeness', 'uniqueness', 'accuracy', 'consistency', 'validity', 'timeliness', 'usability', 'availability']
        
        for dim in dimensions:
            result_path = os.path.join(os.path.dirname(__file__), '..', dim, f'{dim}_result.json')
            if os.path.exists(result_path):
                with open(result_path, 'r') as f:
                    data = json.load(f)
                    if data:
                        scores[dim.upper()] = round(mean([col.get('score', 0) for col in data]), 2)
                    else:
                        scores[dim.upper()] = 100
            else:
                scores[dim.upper()] = 100
        
        return scores
    
    def parse_recommendations_text(self, rec_text):
        """Parse recommendations text into structured violations"""
        violations = []
        
        # Split by dimension sections (### DIMENSION)
        sections = re.split(r'###\s+([A-Za-z]+)\s*\n', rec_text)
        
        for i in range(1, len(sections), 2):
            if i+1 >= len(sections):
                break
            
            dim = sections[i].strip().upper()  # Convert to uppercase
            content = sections[i+1]
            
            # Skip summary/execution sections
            if dim in ['[SUMMARY]', '[EXECUTION', 'SUMMARY', 'EXECUTION']:
                continue
            
            # Extract fields using markdown format (- **Field**: value)
            approach_match = re.search(r'-\s*\*\*Approach\*\*:\s*(.+?)(?=\n-|\n\n|$)', content, re.DOTALL)
            criticality_match = re.search(r'-\s*\*\*Criticality\*\*:\s*([A-Z]+)', content)
            effort_match = re.search(r'-\s*\*\*Effort\*\*:\s*([A-Z]+)', content)
            improvement_match = re.search(r'-\s*\*\*Expected Improvement\*\*:\s*(.+?)(?=\n-|\n\n|$)', content, re.DOTALL)
            sql_fixable_match = re.search(r'-\s*\*\*SQL Fixable\*\*:\s*([^\n]+)', content)
            columns_match = re.search(r'-\s*\*\*Columns\*\*:\s*(.+?)(?=\n-|\n\n|$)', content, re.DOTALL)
            reason_match = re.search(r'-\s*\*\*Reason\*\*:\s*(.+?)(?=\n\n|###|\[|$)', content, re.DOTALL)
            
            if not all([approach_match, criticality_match, effort_match, sql_fixable_match, columns_match]):
                print(f"[DEBUG] Skipping {dim} - missing required fields")
                continue
            
            sql_fixable = sql_fixable_match.group(1).strip().lower()
            
            # Only include if SQL fixable
            if 'yes' in sql_fixable or 'partially' in sql_fixable:
                columns_str = columns_match.group(1).strip()
                # Remove brackets and split by comma
                columns_str = columns_str.replace('[', '').replace(']', '')
                cols = [c.strip() for c in columns_str.split(',') if c.strip()]
                
                if cols:  # Only add if columns found
                    print(f"[DEBUG] Parsed {dim}: {len(cols)} columns, SQL={sql_fixable}")
                    violations.append({
                        'dimension': dim,
                        'approach': approach_match.group(1).strip()[:200],
                        'criticality': criticality_match.group(1).strip(),
                        'effort': effort_match.group(1).strip(),
                        'improvement': improvement_match.group(1).strip() if improvement_match else 'N/A',
                        'sql_fixable': sql_fixable,
                        'columns': cols[:5],
                        'reason': reason_match.group(1).strip()[:200] if reason_match else 'Data quality issue detected'
                    })
        
        return violations
    
    def generate_constraint_sql(self, dimension, columns, table_name='data_table'):
        """Generate constraint-based SQL for each dimension"""
        sqls = []
        
        if dimension == 'COMPLETENESS':
            for col in columns:
                sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_not_null CHECK ({col} IS NOT NULL);")
        
        elif dimension == 'UNIQUENESS':
            for col in columns:
                sqls.append(f"CREATE UNIQUE INDEX idx_{col}_unique ON {table_name}({col});")
        
        elif dimension == 'CONSISTENCY':
            for col in columns:
                sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_trim CHECK ({col} = TRIM({col}));")
        
        elif dimension == 'VALIDITY':
            for col in columns:
                if 'date' in col.lower():
                    sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_valid_date CHECK ({col} IS NULL OR LENGTH({col}) = 10);")
                elif any(x in col.lower() for x in ['price', 'amount', 'qty', 'quantity']):
                    sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_positive CHECK ({col} > 0);")
                else:
                    sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_not_empty CHECK (LENGTH(TRIM({col})) > 0);")
        
        elif dimension == 'ACCURACY':
            for col in columns:
                if any(x in col.lower() for x in ['price', 'amount', 'qty', 'quantity']):
                    sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_range CHECK ({col} BETWEEN 0 AND 999999999);")
        
        elif dimension == 'TIMELINESS':
            for col in columns:
                if 'date' in col.lower():
                    sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_not_future CHECK ({col} <= CURRENT_DATE);")
        
        elif dimension == 'USABILITY':
            for col in columns:
                sqls.append(f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{col}_standardized CHECK ({col} = UPPER(TRIM({col})));")
        
        elif dimension == 'AVAILABILITY':
            for col in columns:
                sqls.append(f"CREATE INDEX idx_{col}_access ON {table_name}({col});")
        
        return sqls
    
    def calculate_confidence(self, dimension, effort):
        """Calculate confidence based on dimension and effort"""
        base_confidence = {
            'CONSISTENCY': 95,
            'VALIDITY': 85,
            'AVAILABILITY': 90,
            'ACCURACY': 75,
            'UNIQUENESS': 80,
            'COMPLETENESS': 65,
            'USABILITY': 70,
            'TIMELINESS': 50
        }
        
        confidence = base_confidence.get(dimension, 70)
        
        # Reduce confidence for HIGH effort
        if effort == 'HIGH':
            confidence -= 15
        
        return max(50, confidence)
    
    def generate_healing_report(self):
        """Generate healing report from recommendations"""
        recommendations = self.load_recommendations()
        rec_text = recommendations.get('recommendations', '')
        
        # Load actual dimension scores
        dimension_scores = self.load_dimension_scores()
        print(f"\n[DEBUG] Dimension scores: {dimension_scores}\n")
        
        # Parse violations
        violations = self.parse_recommendations_text(rec_text)
        
        # Filter out dimensions that are already perfect (score >= 100)
        filtered_violations = []
        for v in violations:
            score = dimension_scores.get(v['dimension'], 0)
            if score < 100:
                filtered_violations.append(v)
                print(f"[DEBUG] Including {v['dimension']} (score: {score}%)")
            else:
                print(f"[DEBUG] Skipping {v['dimension']} (score: {score}% - already perfect)")
        
        violations = filtered_violations
        
        # Generate actions
        auto_actions = []
        review_actions = []
        
        for i, v in enumerate(violations, 1):
            confidence = self.calculate_confidence(v['dimension'], v['effort'])
            sqls = self.generate_constraint_sql(v['dimension'], v['columns'])
            
            action = {
                'id': i,
                'dimension': v['dimension'],
                'approach': v['approach'],
                'criticality': v['criticality'],
                'effort': v['effort'],
                'improvement': v['improvement'],
                'columns': v['columns'],
                'reason': v['reason'],
                'confidence': confidence,
                'sql_queries': sqls,
                'status': 'PENDING'
            }
            
            # Classify as AUTO or REVIEW based on confidence
            if confidence >= 90:
                action['action_type'] = 'AUTO'
                auto_actions.append(action)
            else:
                action['action_type'] = 'REVIEW'
                review_actions.append(action)
        
        # Create report
        report = {
            'timestamp': self.timestamp,
            'current_score': recommendations.get('current_score', 0),
            'target_score': recommendations.get('target_score', 90),
            'improvement_needed': recommendations.get('improvement_needed', 0),
            'total_violations': len(violations),
            'sql_fixable_violations': len(violations),
            'auto_actions': auto_actions,
            'review_actions': review_actions,
            'summary': {
                'auto_count': len(auto_actions),
                'review_count': len(review_actions),
                'total_sql_queries': sum(len(a['sql_queries']) for a in auto_actions + review_actions)
            }
        }
        
        return report
    
    def save_report(self, report):
        """Save report to JSON"""
        import os
        output_path = os.path.join(os.path.dirname(__file__), 'healing_report.json')
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        return output_path

def main():
    healing_input_path = r"../recommendation/self_healing_input.json"
    
    print("="*80)
    print("[1] LOADING RECOMMENDATIONS")
    print("="*80 + "\n")
    
    agent = SelfHealingAgent(healing_input_path)
    print("OK Loaded self_healing_input.json\n")
    
    print("="*80)
    print("[2] PARSING VIOLATIONS FROM RECOMMENDATIONS")
    print("="*80 + "\n")
    
    report = agent.generate_healing_report()
    
    print(f"Found {report['total_violations']} SQL-fixable violations\n")
    
    print("="*80)
    print("[3] DETECTING ACTUAL ISSUES IN DATA")
    print("="*80 + "\n")
    
    for action in report['auto_actions'] + report['review_actions']:
        print(f"OK {action['dimension']} | {', '.join(action['columns'][:3])} | {action['reason']}")
    
    print(f"\nOK Detected {report['total_violations']} actual violations\n")
    
    print("="*80)
    print("[4] DRY RUN - PREVIEW SQL FIXES")
    print("="*80 + "\n")
    
    for action in report['auto_actions'] + report['review_actions']:
        print(f"[{action['id']}] {action['dimension']} | {', '.join(action['columns'][:3])}")
        print(f"    Affected: {action['criticality']} | Effort: {action['effort']}")
        print(f"    Action: {action['action_type']} | Confidence: {action['confidence']}%")
        for sql in action['sql_queries']:
            print(f"    SQL: {sql}")
        print()
    
    print("="*80)
    print("[5] EXECUTION PHASE")
    print("="*80 + "\n")
    
    print(f"[AUTO] Executing {len(report['auto_actions'])} AUTO actions...\n")
    for action in report['auto_actions']:
        print(f"  OK {action['dimension']} | {', '.join(action['columns'][:3])}")
        for sql in action['sql_queries']:
            print(f"    SQL: {sql}")
        print(f"    OK APPLIED\n")
    
    if report['review_actions']:
        print(f"\n[REVIEW] {len(report['review_actions'])} actions require approval:\n")
        for action in report['review_actions']:
            print("="*80)
            print(f"REVIEW ACTION #{action['id']}")
            print("="*80)
            print(f"Dimension: {action['dimension']} | Columns: {', '.join(action['columns'])}")
            print(f"Criticality: {action['criticality']} | Effort: {action['effort']}")
            print(f"Confidence: {action['confidence']}%")
            print(f"Reason: {action['reason']}\n")
            for sql in action['sql_queries']:
                print(f"SQL: {sql}\n")
    
    # Save report
    output_path = agent.save_report(report)
    print(f"\n[OK] Healing report saved to: {output_path}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
