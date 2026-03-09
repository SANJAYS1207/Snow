from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import subprocess
import tempfile
from datetime import datetime
import threading

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_FOLDER = tempfile.gettempdir()
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

def clear_result_files():
    """Clear all result files from previous runs"""
    result_files = [
        'rule_mapping.json',
        'compressor/llm_explanations.json',
        'recommendation/self_healing_input.json',
        'selfhealing/healing_report.json',
        'completeness/completeness_result.json',
        'uniqueness/uniqueness_result.json',
        'accuracy/accuracy_result.json',
        'consistency/consistency_result.json',
        'validity/validity_result.json',
        'timeliness/timeliness_result.json',
        'usability/usability_result.json',
        'availability/availability_result.json'
    ]
    
    for result_file in result_files:
        result_path = os.path.join(BACKEND_DIR, result_file)
        if os.path.exists(result_path):
            try:
                os.remove(result_path)
            except:
                pass

@app.on_event("startup")
async def startup_event():
    """Clear old results on server startup"""
    clear_result_files()
    print("[OK] Cleared old result files")

@app.post("/api/analyze")
async def analyze_csv(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith('.csv'):
            return JSONResponse({'error': 'Only CSV files allowed'}, status_code=400)
        
        clear_result_files()
        
        filename = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        with open(filepath, 'wb') as f:
            f.write(await file.read())
        
        def run_analysis():
            print(f"\n{'='*80}")
            print(f"[BACKEND] Starting analysis for: {filename}")
            print(f"{'='*80}\n")
            result = subprocess.run(
                ['python', 'main.py', filepath], 
                cwd=BACKEND_DIR,
                capture_output=False,
                text=True
            )
            if result.returncode == 0:
                print(f"\n[BACKEND] Analysis completed successfully\n")
            else:
                print(f"\n[BACKEND] Analysis failed with code {result.returncode}\n")
        
        thread = threading.Thread(target=run_analysis, daemon=True)
        thread.start()
        
        return {'status': 'started', 'message': 'Analysis started'}
    
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=500)

@app.get("/api/stage/{stage_name}")
async def get_stage(stage_name: str):
    try:
        if stage_name == 'rule_mapping':
            path = os.path.join(BACKEND_DIR, 'rule_mapping.json')
        elif stage_name == 'llm_explanations':
            path = os.path.join(BACKEND_DIR, 'compressor', 'llm_explanations.json')
        elif stage_name == 'recommendations':
            path = os.path.join(BACKEND_DIR, 'recommendation', 'self_healing_input.json')
        elif stage_name == 'healing':
            path = os.path.join(BACKEND_DIR, 'selfhealing', 'healing_report.json')
        else:
            path = os.path.join(BACKEND_DIR, stage_name, f"{stage_name}_result.json")
        
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
        return {'status': 'not_ready'}
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=500)

@app.get("/api/health")
async def health():
    return {'status': 'ok'}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)
