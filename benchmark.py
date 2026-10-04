import time
import argparse
import requests
import json
import math
import random

def percentile(data, percent):
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (percent / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return d0 + d1

def run_benchmarks(url, count):
    # Find current primary first
    target_url = url
    try:
        resp = requests.get(f"{url}/role", timeout=2.0)
        if resp.status_code == 200:
            info = resp.json()
            if info.get("role") == "replica" and info.get("primary"):
                target_url = info.get("primary")
                print(f"Target node is a replica. Redirecting requests to active primary: {target_url}")
    except Exception:
        print(f"Could not connect to {url} to fetch role. Proceeding with raw url.")

    print(f"Starting benchmark of {count} SETs and {count} GETs against {target_url}...")

    set_latencies = []
    get_latencies = []
    
    set_success = 0
    set_failures = 0
    get_success = 0
    get_failures = 0

    # 1. Run SET Benchmarks
    start_time_set = time.time()
    for i in range(count):
        key = f"key_{i}"
        value = f"value_{i}"
        
        req_start = time.time()
        try:
            resp = requests.post(f"{target_url}/set", json={"key": key, "value": value}, timeout=5.0)
            latency = (time.time() - req_start) * 1000.0 # in ms
            set_latencies.append(latency)
            if resp.status_code == 200:
                set_success += 1
            else:
                set_failures += 1
        except Exception:
            latency = (time.time() - req_start) * 1000.0
            set_latencies.append(latency)
            set_failures += 1
    total_time_set = time.time() - start_time_set

    # 2. Run GET Benchmarks
    # We query a mix of keys
    start_time_get = time.time()
    for i in range(count):
        key = f"key_{random.randint(0, count - 1)}"
        
        req_start = time.time()
        try:
            resp = requests.get(f"{target_url}/get", params={"key": key}, timeout=5.0)
            latency = (time.time() - req_start) * 1000.0 # in ms
            get_latencies.append(latency)
            if resp.status_code in [200, 404]:
                get_success += 1
            else:
                get_failures += 1
        except Exception:
            latency = (time.time() - req_start) * 1000.0
            get_latencies.append(latency)
            get_failures += 1
    total_time_get = time.time() - start_time_get

    # Calculate stats
    def get_stats(latencies, total_time, success, failures):
        if not latencies:
            return {}
        return {
            "p50": percentile(latencies, 50.0),
            "p95": percentile(latencies, 95.0),
            "p99": percentile(latencies, 99.0),
            "min": min(latencies),
            "max": max(latencies),
            "avg": sum(latencies) / len(latencies),
            "throughput": len(latencies) / total_time,
            "success": success,
            "failures": failures,
            "total_time_sec": total_time
        }

    set_stats = get_stats(set_latencies, total_time_set, set_success, set_failures)
    get_stats_data = get_stats(get_latencies, total_time_get, get_success, get_failures)

    print("\nSET Results:")
    print(f"  p50: {set_stats['p50']:.2f} ms")
    print(f"  p95: {set_stats['p95']:.2f} ms")
    print(f"  p99: {set_stats['p99']:.2f} ms")
    print(f"  Throughput: {set_stats['throughput']:.2f} req/s")

    print("\nGET Results:")
    print(f"  p50: {get_stats_data['p50']:.2f} ms")
    print(f"  p95: {get_stats_data['p95']:.2f} ms")
    print(f"  p99: {get_stats_data['p99']:.2f} ms")
    print(f"  Throughput: {get_stats_data['throughput']:.2f} req/s")

    # We downsample latencies to 100 points for charts to keep file size small and render fast
    def downsample(data, points=100):
        if len(data) <= points:
            return data
        step = len(data) / points
        return [data[int(i * step)] for i in range(points)]

    chart_data = {
        "set_latencies": downsample(set_latencies),
        "get_latencies": downsample(get_latencies),
        "set_stats": set_stats,
        "get_stats": get_stats_data,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    generate_dashboard(chart_data)

def generate_dashboard(data):
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Distributed KV Store Latency Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg-color: #0b0f19;
            --card-bg: #1e293b;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --primary: #3b82f6;
            --primary-glow: rgba(59, 130, 246, 0.15);
            --secondary: #10b981;
            --secondary-glow: rgba(16, 185, 129, 0.15);
            --danger: #ef4444;
            --border: #334155;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-main);
            padding: 2rem;
            min-height: 100vh;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        header {{
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border);
            padding-bottom: 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        h1 {{
            font-size: 2rem;
            font-weight: 700;
            background: linear-gradient(to right, #60a5fa, #34d399);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .timestamp {{
            color: var(--text-muted);
            font-size: 0.9rem;
        }}

        .grid-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .card {{
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            transition: transform 0.2s, border-color 0.2s;
        }}

        .card:hover {{
            transform: translateY(-2px);
            border-color: #475569;
        }}

        .card-title {{
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }}

        .card-value {{
            font-size: 1.8rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }}

        .card-desc {{
            font-size: 0.8rem;
            color: var(--text-muted);
        }}

        .chart-container {{
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            height: 400px;
            position: relative;
        }}

        .grid-charts {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .section-title {{
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
        }}

        .dot-set {{ background-color: var(--primary); }}
        .dot-get {{ background-color: var(--secondary); }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>Antigravity Distributed KV Store</h1>
                <p style="color: var(--text-muted); margin-top: 0.25rem;">Performance & Latency Benchmark Results</p>
            </div>
            <div class="timestamp">Executed on: <span id="timestamp"></span></div>
        </header>

        <div class="section-title"><span class="dot dot-set"></span> SET (Write) Performance</div>
        <div class="grid-stats">
            <div class="card">
                <div class="card-title">Throughput</div>
                <div class="card-value" id="set-throughput" style="color: var(--primary);">0 req/s</div>
                <div class="card-desc">Requests completed per second</div>
            </div>
            <div class="card">
                <div class="card-title">p50 Latency</div>
                <div class="card-value" id="set-p50">0.00 ms</div>
                <div class="card-desc">50% of requests are faster than this</div>
            </div>
            <div class="card">
                <div class="card-title">p95 Latency</div>
                <div class="card-value" id="set-p95">0.00 ms</div>
                <div class="card-desc">95% of requests are faster than this</div>
            </div>
            <div class="card">
                <div class="card-title">p99 Latency</div>
                <div class="card-value" id="set-p99" style="color: #f59e0b;">0.00 ms</div>
                <div class="card-desc">99% of requests are faster than this</div>
            </div>
        </div>

        <div class="section-title"><span class="dot dot-get"></span> GET (Read) Performance</div>
        <div class="grid-stats">
            <div class="card">
                <div class="card-title">Throughput</div>
                <div class="card-value" id="get-throughput" style="color: var(--secondary);">0 req/s</div>
                <div class="card-desc">Requests completed per second</div>
            </div>
            <div class="card">
                <div class="card-title">p50 Latency</div>
                <div class="card-value" id="get-p50">0.00 ms</div>
                <div class="card-desc">50% of requests are faster than this</div>
            </div>
            <div class="card">
                <div class="card-title">p95 Latency</div>
                <div class="card-value" id="get-p95">0.00 ms</div>
                <div class="card-desc">95% of requests are faster than this</div>
            </div>
            <div class="card">
                <div class="card-title">p99 Latency</div>
                <div class="card-value" id="get-p99" style="color: #f59e0b;">0.00 ms</div>
                <div class="card-desc">99% of requests are faster than this</div>
            </div>
        </div>

        <div class="grid-charts">
            <div class="chart-container">
                <canvas id="percentileChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="timelineChart"></canvas>
            </div>
        </div>
    </div>

    <script>
        const rawData = {json.dumps(data)};
        
        document.getElementById('timestamp').innerText = rawData.timestamp;
        
        // Populate SET Stats
        document.getElementById('set-throughput').innerText = rawData.set_stats.throughput.toFixed(2) + ' req/s';
        document.getElementById('set-p50').innerText = rawData.set_stats.p50.toFixed(2) + ' ms';
        document.getElementById('set-p95').innerText = rawData.set_stats.p95.toFixed(2) + ' ms';
        document.getElementById('set-p99').innerText = rawData.set_stats.p99.toFixed(2) + ' ms';
        
        // Populate GET Stats
        document.getElementById('get-throughput').innerText = rawData.get_stats.throughput.toFixed(2) + ' req/s';
        document.getElementById('get-p50').innerText = rawData.get_stats.p50.toFixed(2) + ' ms';
        document.getElementById('get-p95').innerText = rawData.get_stats.p95.toFixed(2) + ' ms';
        document.getElementById('get-p99').innerText = rawData.get_stats.p99.toFixed(2) + ' ms';

        // Chart.js Configuration
        const ctxPercentile = document.getElementById('percentileChart').getContext('2d');
        const percentileChart = new Chart(ctxPercentile, {{
            type: 'bar',
            data: {{
                labels: ['Min', 'Average', 'p50 (Median)', 'p95', 'p99', 'Max'],
                datasets: [
                    {{
                        label: 'SET (Write) Latency (ms)',
                        data: [
                            rawData.set_stats.min,
                            rawData.set_stats.avg,
                            rawData.set_stats.p50,
                            rawData.set_stats.p95,
                            rawData.set_stats.p99,
                            rawData.set_stats.max
                        ],
                        backgroundColor: '#3b82f6',
                        borderColor: '#2563eb',
                        borderWidth: 1
                    }},
                    {{
                        label: 'GET (Read) Latency (ms)',
                        data: [
                            rawData.get_stats.min,
                            rawData.get_stats.avg,
                            rawData.get_stats.p50,
                            rawData.get_stats.p95,
                            rawData.get_stats.p99,
                            rawData.get_stats.max
                        ],
                        backgroundColor: '#10b981',
                        borderColor: '#059669',
                        borderWidth: 1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Latency Metrics Comparison',
                        color: '#f8fafc',
                        font: {{ size: 16, weight: 'bold' }}
                    }},
                    legend: {{
                        labels: {{ color: '#f8fafc' }}
                    }}
                }},
                scales: {{
                    y: {{
                        title: {{ display: true, text: 'Latency (ms)', color: '#94a3b8' }},
                        grid: {{ color: '#334155' }},
                        ticks: {{ color: '#94a3b8' }},
                        type: 'logarithmic'
                    }},
                    x: {{
                        grid: {{ display: false }},
                        ticks: {{ color: '#94a3b8' }}
                    }}
                }}
            }}
        }});

        const ctxTimeline = document.getElementById('timelineChart').getContext('2d');
        const labels = Array.from({{ length: rawData.set_latencies.length }}, (_, i) => `${{(i * 10).toFixed(0)}}%`);
        const timelineChart = new Chart(ctxTimeline, {{
            type: 'line',
            data: {{
                labels: labels,
                datasets: [
                    {{
                        label: 'SET Latency (ms)',
                        data: rawData.set_latencies,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        tension: 0.1,
                        fill: true
                    }},
                    {{
                        label: 'GET Latency (ms)',
                        data: rawData.get_latencies,
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.1,
                        fill: true
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Latency Profile over Benchmark Execution',
                        color: '#f8fafc',
                        font: {{ size: 16, weight: 'bold' }}
                    }},
                    legend: {{
                        labels: {{ color: '#f8fafc' }}
                    }}
                }},
                scales: {{
                    y: {{
                        title: {{ display: true, text: 'Latency (ms)', color: '#94a3b8' }},
                        grid: {{ color: '#334155' }},
                        ticks: {{ color: '#94a3b8' }}
                    }},
                    x: {{
                        title: {{ display: true, text: 'Benchmark Progress', color: '#94a3b8' }},
                        grid: {{ display: false }},
                        ticks: {{ color: '#94a3b8' }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    with open("dashboard.html", "w") as f:
        f.write(html_content)
    print(f"Successfully generated HTML Latency Dashboard: dashboard.html")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, default="http://localhost:5000")
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()

    run_benchmarks(args.url, args.count)
