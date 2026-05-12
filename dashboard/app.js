const consoleOutput = document.getElementById('console-output');

function logToConsole(text, type = 'info') {
    const entry = document.createElement('div');
    entry.style.opacity = '0.9';
    entry.style.marginBottom = '4px';
    
    // Auto-detect color based on content
    let color = '#f5f5f7'; // default
    const lowerText = text.toLowerCase();
    
    if (type === 'error' || lowerText.includes('error') || lowerText.includes('failed')) color = '#ff3b30';
    else if (type === 'success' || lowerText.includes('done') || lowerText.includes('success') || lowerText.includes('finished')) color = '#34c759';
    else if (lowerText.startsWith('>') || lowerText.startsWith('*') || lowerText.includes('executing')) color = '#0071e3';
    
    entry.style.color = color;
    entry.textContent = text.startsWith('>') ? text : `> ${text}`;
    consoleOutput.appendChild(entry);
    consoleOutput.scrollTop = consoleOutput.scrollHeight;
}

function clearConsole() {
    consoleOutput.innerHTML = '';
}

async function apiCall(action, params = {}) {
    try {
        const response = await fetch('/', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action, ...params })
        });
        return await response.json();
    } catch (e) {
        logToConsole(`Error: ${e.message}`, 'error');
        return { status: 'error', output: e.message };
    }
}

function showTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    
    document.getElementById(`tab-${tabName}`).classList.add('active');
    
    // Find and highlight the correct tab button programmatically
    const buttons = document.querySelectorAll('.tab-btn');
    buttons.forEach(btn => {
        if (btn.textContent.toLowerCase().includes(tabName.toLowerCase())) {
            btn.classList.add('active');
        }
    });
}

async function runCommand(command, label) {
    showTab('console');
    logToConsole(`Executing: ${label}...`);
    document.getElementById('console-spinner').style.display = 'inline-block';
    
    await apiCall('run_command', { command });
    
    // The pollLogs will handle clearing the spinner when it sees output
    // but we add a safety timeout here
    return { status: 'ok' };
}

async function pollLogs() {
    const logRes = await apiCall('get_logs');
    if (logRes.output) {
        document.getElementById('console-spinner').style.display = 'none';
        const lines = logRes.output.split('\n');
        lines.forEach(line => {
            if (line.trim()) logToConsole(line);
        });
    }
}

async function fetchMinikubeStatus() {
    const res = await apiCall('minikube_status');
    const badge = document.getElementById('m-status-badge');
    const text = document.getElementById('m-status-text');
    
    const status = res.output || "Offline";
    text.textContent = `Minikube: ${status}`;
    
    if (status.toLowerCase().includes('running')) {
        badge.className = 'status-badge online';
    } else {
        badge.className = 'status-badge offline';
    }
}

// Initial fetch and poll
fetchPods();
fetchMinikubeStatus();
setInterval(fetchPods, 5000);
setInterval(fetchMinikubeStatus, 5000);
setInterval(pollLogs, 1000);

async function fetchPods() {
    const res = await apiCall('get_pods');
    if (res.status === 'ok' && res.output && res.output.items) {
        renderPods(res.output.items);
    }
}

function renderPods(pods) {
    const podsList = document.getElementById('pods-list');
    podsList.innerHTML = '';
    
    pods.forEach(pod => {
        const status = pod.status.phase.toLowerCase();
        const name = pod.metadata.name.split('-').slice(0, -2).join('-') || pod.metadata.name;
        
        // Calculate Ready count (e.g., 1/1)
        const containerStatuses = pod.status.containerStatuses || [];
        const readyCount = containerStatuses.filter(s => s.ready).length;
        const totalCount = containerStatuses.length;
        const readyStr = `${readyCount}/${totalCount}`;
        
        // Calculate Age (simplified)
        const startTime = new Date(pod.metadata.creationTimestamp);
        const diffMs = new Date() - startTime;
        const diffMin = Math.floor(diffMs / 60000);
        const ageStr = diffMin > 60 ? `${Math.floor(diffMin/60)}h` : `${diffMin}m`;

        const restarts = containerStatuses.length > 0 ? containerStatuses[0].restartCount : 0;

        const row = document.createElement('div');
        row.className = 'pod-row';
        row.innerHTML = `
            <div class="col-name">${name}</div>
            <div class="col-ready">${readyStr}</div>
            <div class="col-status">
                <span class="status-dot dot-${status}"></span>
                ${pod.status.phase}
            </div>
            <div class="col-restarts">${restarts}</div>
            <div class="col-age">${ageStr}</div>
        `;
        podsList.appendChild(row);
    });
}

async function runBuildAll() {
    const commands = [
        { cmd: "docker build -t mapreduce-manager:latest -f manager-service/Dockerfile .", label: "Build Manager" },
        { cmd: "docker build -t mapreduce-ui:latest -f ui-service/Dockerfile .", label: "Build UI" },
        { cmd: "docker build -t worker-service:latest -f worker-service/Dockerfile .", label: "Build Worker" },
        { cmd: "minikube image load mapreduce-manager:latest", label: "Load Manager" },
        { cmd: "minikube image load mapreduce-ui:latest", label: "Load UI" },
        { cmd: "minikube image load worker-service:latest", label: "Load Worker" }
    ];

    for (const item of commands) {
        const res = await runCommand(item.cmd, item.label);
        if (res.status !== 'ok') break;
    }
    fetchPods();
}

async function runRestartAll() {
    await runCommand("kubectl rollout restart statefulset mapreduce-manager", "Restart Manager");
    await runCommand("kubectl rollout restart deployment mapreduce-ui", "Restart UI");
    fetchPods();
}

const activePF = {};

async function togglePortForward(service, local, remote) {
    const btn = document.getElementById(`pf-${service.replace('mapreduce-', '')}`);
    
    if (activePF[service]) {
        await apiCall('stop_port_forward', { service });
        activePF[service] = false;
        btn.classList.remove('active');
        logToConsole(`Stopped port-forward for ${service}`);
    } else {
        await apiCall('start_port_forward', { service, local_port: local, remote_port: remote });
        activePF[service] = true;
        btn.classList.add('active');
        logToConsole(`Started port-forward for ${service} on ${local}`);
    }
}

async function runFullSetup() {
    showTab('console');
    clearConsole();
    logToConsole("✨ STARTING FULL MAGIC SETUP ✨", "success");
    
    // 1. Minikube Start
    await runCommand("minikube start --driver=docker", "Minikube Start");
    
    // 2. K8s Apply
    await runCommand("kubectl apply -f k8s/", "Apply K8s Manifests");
    
    // 3. Build & Load All
    await runBuildAll();
    
    // 4. Restart Deployments
    await runRestartAll();
    
    // 5. Start all Port Forwards
    logToConsole("Establishing connections...");
    if (!activePF['mapreduce-ui']) await togglePortForward('mapreduce-ui', 8001, 8000);
    if (!activePF['mapreduce-keycloak']) await togglePortForward('mapreduce-keycloak', 8080, 8080);
    if (!activePF['mapreduce-minio']) await togglePortForward('mapreduce-minio', 9000, 9000);
    
    logToConsole("✅ FULL SETUP COMPLETE! Everything is ready.", "success");
}

async function openAllPorts() {
    logToConsole("Opening all system ports...");
    if (!activePF['mapreduce-ui']) await togglePortForward('mapreduce-ui', 8001, 8000);
    if (!activePF['mapreduce-keycloak']) await togglePortForward('mapreduce-keycloak', 8080, 8080);
    if (!activePF['mapreduce-minio']) await togglePortForward('mapreduce-minio', 9000, 9000);
    logToConsole("All ports are now being forwarded.", "success");
}

async function closeAllPorts() {
    logToConsole("Closing all active connections...");
    for (const service in activePF) {
        if (activePF[service]) {
            await togglePortForward(service); 
        }
    }
    logToConsole("All connections closed.", "success");
}

// Initial fetch and poll
fetchPods();
setInterval(fetchPods, 5000);
