from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from app.rate_limiter.limiter import is_allowed
from app.db.database import SessionLocal, init_db
from app.db.models import JobRecord
from app.redis_client import r
from app.logging_config import setup_logging
from app.metrics import timed, latency_stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    init_db()               # create tables on startup
    yield


app = FastAPI(title="Distributed Job Queue & Rate Limiter", lifespan=lifespan)

class CustomJobRequest(BaseModel):
    task_type: str
    payload: dict

@app.get("/check/rate_limit/{user_id}")
def check_rate_limit(user_id: str):
    with timed("rate_limit"):
        allowed = is_allowed(user_id)
    if allowed:
        return {"message": "Request allowed"}
    raise HTTPException(status_code=429, detail="Rate limit exceeded. Please try again later.")

@app.get("/api/metrics")
def get_metrics():
    db = SessionLocal()
    try:
        completed = db.query(JobRecord).filter(JobRecord.status == "completed").count()
        failed = db.query(JobRecord).filter(JobRecord.status == "failed").count()
        retrying = db.query(JobRecord).filter(JobRecord.status == "retrying").count()
        total_logged = db.query(JobRecord).count()
        
        queue_size = r.llen("job_queue")
        dlq_size = r.llen("dead_letter_queue")
        
        return {
            "queue_size": queue_size,
            "dlq_size": dlq_size,
            "completed": completed,
            "failed": failed,
            "retrying": retrying,
            "total_logged": total_logged,
            "latency": {
                "rate_limit": latency_stats("rate_limit"),   # limiter decision latency
                "job_process": latency_stats("job_process"),  # worker execution time
                "job_e2e": latency_stats("job_e2e"),          # enqueue -> completion
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/jobs")
def get_recent_jobs():
    db = SessionLocal()
    try:
        jobs = db.query(JobRecord).order_by(JobRecord.updated_at.desc()).limit(10).all()
        return [
            {
                "job_id": job.job_id,
                "task_type": job.task_type,
                "status": job.status,
                "retry_count": job.retry_count,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            }
            for job in jobs
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.post("/api/submit_test_job")
def submit_test_job():
    try:
        from app.job_queue.producer import submit_job
        import random
        task_types = ["send_email", "process_payment", "generate_pdf", "sync_data"]
        task_type = random.choice(task_types)
        job_id = submit_job(task_type, {"random_payload": random.randint(100, 999)})
        return {"status": "success", "job_id": job_id, "task_type": task_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/submit_custom_job")
def submit_custom_job(request: CustomJobRequest):
    try:
        from app.job_queue.producer import submit_job
        job_id = submit_job(request.task_type, request.payload)
        return {"status": "success", "job_id": job_id}
    except ValueError as ve:
        raise HTTPException(status_code=409, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard", response_class=HTMLResponse)
def serve_dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Distributed Job Queue Monitor</title>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            :root {
                --bg-primary: #0f172a;
                --bg-secondary: #1e293b;
                --accent-blue: #38bdf8;
                --accent-green: #34d399;
                --accent-red: #f87171;
                --accent-yellow: #fbbf24;
                --text-main: #f8fafc;
                --text-muted: #94a3b8;
            }
            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
                font-family: 'Outfit', sans-serif;
            }
            body {
                background-color: var(--bg-primary);
                color: var(--text-main);
                min-height: 100vh;
                padding: 2rem;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2rem;
                border-bottom: 1px solid #334155;
                padding-bottom: 1.5rem;
            }
            h1 {
                font-size: 2.2rem;
                font-weight: 800;
                background: linear-gradient(135deg, #38bdf8, #818cf8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .live-indicator {
                display: flex;
                align-items: center;
                gap: 0.5rem;
                font-size: 0.9rem;
                color: var(--accent-green);
                background: rgba(52, 211, 153, 0.1);
                padding: 0.4rem 0.8rem;
                border-radius: 9999px;
                border: 1px solid rgba(52, 211, 153, 0.2);
            }
            .pulse {
                width: 8px;
                height: 8px;
                background-color: var(--accent-green);
                border-radius: 50%;
                animation: blink 1.5s infinite;
            }
            @keyframes blink {
                0% { opacity: 0.3; }
                50% { opacity: 1; }
                100% { opacity: 0.3; }
            }
            .grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 1.5rem;
                margin-bottom: 2.5rem;
            }
            .card {
                background-color: var(--bg-secondary);
                border: 1px solid #334155;
                border-radius: 1rem;
                padding: 1.5rem;
                transition: transform 0.2s ease, border-color 0.2s ease;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            }
            .card:hover {
                transform: translateY(-4px);
                border-color: #475569;
            }
            .card-title {
                font-size: 0.9rem;
                color: var(--text-muted);
                text-transform: uppercase;
                letter-spacing: 0.05em;
                margin-bottom: 0.5rem;
            }
            .card-value {
                font-size: 2.5rem;
                font-weight: 800;
                margin-bottom: 0.5rem;
            }
            .queue { color: var(--accent-blue); }
            .dlq { color: var(--accent-red); }
            .success { color: var(--accent-green); }
            .retrying { color: var(--accent-yellow); }

            .dashboard-body {
                display: grid;
                grid-template-columns: 1fr 2fr;
                gap: 2rem;
                align-items: start;
                margin-bottom: 2rem;
            }
            @media (max-width: 900px) {
                .dashboard-body {
                    grid-template-columns: 1fr;
                }
            }
            .control-panel, .chart-panel, .jobs-panel {
                background-color: var(--bg-secondary);
                border: 1px solid #334155;
                border-radius: 1rem;
                padding: 2rem;
            }
            .panel-title {
                font-size: 1.3rem;
                font-weight: 600;
                margin-bottom: 1.5rem;
                border-bottom: 1px solid #334155;
                padding-bottom: 0.75rem;
            }
            .btn {
                width: 100%;
                background: linear-gradient(135deg, #38bdf8, #0284c7);
                color: var(--text-main);
                border: none;
                padding: 0.75rem;
                font-size: 0.95rem;
                font-weight: 600;
                border-radius: 0.5rem;
                cursor: pointer;
                transition: opacity 0.2s ease, transform 0.1s ease;
                margin-bottom: 1rem;
            }
            .btn:hover { opacity: 0.9; }
            .btn:active { transform: scale(0.98); }
            
            .input-group {
                margin-bottom: 1rem;
                text-align: left;
            }
            .input-group label {
                display: block;
                font-size: 0.85rem;
                color: var(--text-muted);
                margin-bottom: 0.4rem;
            }
            .input-group input, .input-group select {
                width: 100%;
                background-color: var(--bg-primary);
                border: 1px solid #334155;
                color: var(--text-main);
                padding: 0.6rem;
                border-radius: 0.5rem;
                font-size: 0.9rem;
            }
            
            .log-box {
                background-color: var(--bg-primary);
                border: 1px solid #334155;
                border-radius: 0.5rem;
                padding: 0.8rem;
                height: 120px;
                overflow-y: auto;
                font-family: monospace;
                font-size: 0.8rem;
                color: var(--text-muted);
            }
            .log-entry {
                margin-bottom: 0.4rem;
                border-bottom: 1px solid #1e293b;
                padding-bottom: 0.2rem;
            }
            .log-entry.success { color: var(--accent-green); }
            .log-entry.info { color: var(--accent-blue); }

            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 1rem;
                font-size: 0.9rem;
                text-align: left;
            }
            th, td {
                padding: 0.75rem;
                border-bottom: 1px solid #334155;
            }
            th {
                color: var(--text-muted);
                font-weight: 600;
                text-transform: uppercase;
                font-size: 0.8rem;
            }
            .badge {
                padding: 0.25rem 0.5rem;
                border-radius: 0.25rem;
                font-size: 0.75rem;
                font-weight: 600;
                text-transform: uppercase;
            }
            .badge.completed { background: rgba(52, 211, 153, 0.1); color: var(--accent-green); border: 1px solid rgba(52, 211, 153, 0.2); }
            .badge.failed { background: rgba(248, 113, 113, 0.1); color: var(--accent-red); border: 1px solid rgba(248, 113, 113, 0.2); }
            .badge.retrying { background: rgba(251, 191, 36, 0.1); color: var(--accent-yellow); border: 1px solid rgba(251, 191, 36, 0.2); }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div>
                    <h1>System Monitor</h1>
                    <p style="color: var(--text-muted); font-size: 0.95rem; margin-top: 0.25rem;">Distributed Job Queue & Rate Limiter Real-Time Dashboard</p>
                </div>
                <div class="live-indicator">
                    <div class="pulse"></div>
                    Live Update
                </div>
            </header>

            <div class="grid">
                <div class="card">
                    <div class="card-title">Job Queue</div>
                    <div class="card-value queue" id="val-queue">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">Active Redis list capacity</p>
                </div>
                <div class="card">
                    <div class="card-title">Completed Jobs</div>
                    <div class="card-value success" id="val-completed">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">Successfully processed (Postgres)</p>
                </div>
                <div class="card">
                    <div class="card-title">Failed (DLQ)</div>
                    <div class="card-value dlq" id="val-dlq">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">Exceeded retries / Dead Letter</p>
                </div>
                <div class="card">
                    <div class="card-title">Retrying</div>
                    <div class="card-value retrying" id="val-retrying">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">Currently in retry loop</p>
                </div>
            </div>

            <div class="grid">
                <div class="card">
                    <div class="card-title">Rate-limit p50</div>
                    <div class="card-value success" id="val-p50">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">ms · median decision latency</p>
                </div>
                <div class="card">
                    <div class="card-title">Rate-limit p95</div>
                    <div class="card-value queue" id="val-p95">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">ms</p>
                </div>
                <div class="card">
                    <div class="card-title">Rate-limit p99</div>
                    <div class="card-value retrying" id="val-p99">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">ms · tail latency</p>
                </div>
                <div class="card">
                    <div class="card-title">Checks Measured</div>
                    <div class="card-value" id="val-lat-count">0</div>
                    <p style="font-size: 0.8rem; color: var(--text-muted);">rate-limit calls sampled</p>
                </div>
            </div>

            <div class="dashboard-body">
                <div class="control-panel">
                    <h2 class="panel-title">Manual Job Dispatcher</h2>
                    <div class="input-group">
                        <label for="task_type">Task Type</label>
                        <select id="task_type">
                            <option value="send_email">Email Notification</option>
                            <option value="process_payment">Process Payment</option>
                            <option value="generate_pdf">Generate PDF Reports</option>
                            <option value="sync_data">Data Synchronization</option>
                        </select>
                    </div>
                    <div class="input-group">
                        <label for="payload_recipient">Recipient / User ID</label>
                        <input type="text" id="payload_recipient" placeholder="e.g. user_99" value="user_45">
                    </div>
                    <button class="btn" onclick="submitCustomJob()">Dispatch Job</button>
                    
                    <h3 style="font-size: 0.9rem; color: var(--text-muted); margin-bottom: 0.5rem; text-align: left;">Live Console</h3>
                    <div class="log-box" id="log-box">
                        <div class="log-entry info">System monitor initialized.</div>
                    </div>
                </div>

                <div class="chart-panel">
                    <h2 class="panel-title">System Status Distribution</h2>
                    <div style="position: relative; height: 300px; width: 100%;">
                        <canvas id="metricsChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="jobs-panel">
                <h2 class="panel-title">Recent Database Logs (Last 10 Actions)</h2>
                <div style="overflow-x: auto;">
                    <table>
                        <thead>
                            <tr>
                                <th>Job ID</th>
                                <th>Task Type</th>
                                <th>Status</th>
                                <th>Attempts</th>
                                <th>Created At</th>
                                <th>Completed At</th>
                            </tr>
                        </thead>
                        <tbody id="jobs-table-body">
                            <tr>
                                <td colspan="6" style="text-align: center; color: var(--text-muted);">No jobs processed yet.</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <script>
            let chartInstance = null;

            function addLog(text, type='info') {
                const box = document.getElementById('log-box');
                const entry = document.createElement('div');
                entry.className = `log-entry ${type}`;
                const time = new Date().toLocaleTimeString();
                entry.innerText = `[${time}] ${text}`;
                box.appendChild(entry);
                box.scrollTop = box.scrollHeight;
            }

            async function submitCustomJob() {
                const taskType = document.getElementById('task_type').value;
                const recipient = document.getElementById('payload_recipient').value;
                
                try {
                    const res = await fetch('/api/submit_custom_job', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            task_type: taskType,
                            payload: { recipient_id: recipient }
                        })
                    });
                    const data = await res.json();
                    if (res.ok) {
                        addLog(`Dispatched custom ${taskType} job (ID: ${data.job_id.substring(0,8)}...)`, 'success');
                        fetchMetrics();
                    } else {
                        addLog(`⚠️ Ignored: ${data.detail}`, 'retrying');
                    }
                } catch(e) {
                    addLog('Failed to submit job: ' + e.message, 'dlq');
                }
            }

            async function fetchMetrics() {
                try {
                    // Fetch summary
                    const res = await fetch('/api/metrics');
                    const data = await res.json();
                    
                    document.getElementById('val-queue').innerText = data.queue_size;
                    document.getElementById('val-completed').innerText = data.completed;
                    document.getElementById('val-dlq').innerText = data.dlq_size;
                    document.getElementById('val-retrying').innerText = data.retrying;

                    const lat = (data.latency && data.latency.rate_limit) || {};
                    document.getElementById('val-p50').innerText = lat.p50_ms ?? 0;
                    document.getElementById('val-p95').innerText = lat.p95_ms ?? 0;
                    document.getElementById('val-p99').innerText = lat.p99_ms ?? 0;
                    document.getElementById('val-lat-count').innerText = lat.count ?? 0;

                    updateChart(data);

                    // Fetch recent jobs table
                    const resJobs = await fetch('/api/jobs');
                    const jobs = await resJobs.json();
                    populateJobsTable(jobs);
                } catch (e) {
                    console.error('Error fetching metrics:', e);
                }
            }

            function populateJobsTable(jobs) {
                const body = document.getElementById('jobs-table-body');
                if (jobs.length === 0) {
                    body.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--text-muted);">No jobs processed yet.</td></tr>`;
                    return;
                }
                
                body.innerHTML = jobs.map(job => {
                    const formattedCreated = new Date(job.created_at).toLocaleString();
                    const formattedCompleted = job.completed_at ? new Date(job.completed_at).toLocaleString() : '-';
                    return `
                        <tr>
                            <td style="font-family: monospace; font-size: 0.85rem; color: var(--accent-blue);">${job.job_id}</td>
                            <td>${job.task_type}</td>
                            <td><span class="badge ${job.status}">${job.status}</span></td>
                            <td>${job.retry_count}</td>
                            <td>${formattedCreated}</td>
                            <td>${formattedCompleted}</td>
                        </tr>
                    `;
                }).join('');
            }

            function updateChart(data) {
                const ctx = document.getElementById('metricsChart').getContext('2d');
                const chartData = [data.queue_size, data.completed, data.dlq_size, data.retrying];

                if (!chartInstance) {
                    chartInstance = new Chart(ctx, {
                        type: 'bar',
                        data: {
                            labels: ['Queue Size', 'Completed', 'Dead Letter', 'Retrying'],
                            datasets: [{
                                label: 'Jobs',
                                data: chartData,
                                backgroundColor: ['#38bdf8', '#34d399', '#f87171', '#fbbf24'],
                                borderColor: ['#0284c7', '#059669', '#dc2626', '#d97706'],
                                borderWidth: 1,
                                borderRadius: 8
                            }]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {
                                legend: { display: false }
                            },
                            scales: {
                                y: {
                                    grid: { color: '#334155' },
                                    ticks: { color: '#94a3b8', stepSize: 1 }
                                },
                                x: {
                                    grid: { display: false },
                                    ticks: { color: '#94a3b8' }
                                }
                            }
                        }
                    });
                } else {
                    chartInstance.data.datasets[0].data = chartData;
                    chartInstance.update();
                }
            }

            // Initial fetch
            fetchMetrics();
            // Poll metrics every 2 seconds
            setInterval(fetchMetrics, 2000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)