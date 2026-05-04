
// ═══════════════════════════════════════════════════════════════
//  LANShare Web Client
// ═══════════════════════════════════════════════════════════════

const SERVER = window.location.origin;
const WS_URL = `ws://${window.location.host}/ws/`;

// ── State ──────────────────────────────────────────────────────
let ws = null;
let myId = generateId();
let myName = prompt("Your name on this network:", detectName()) || detectName();
let myPlatform = detectPlatform();
let devices = {};          // id → device
let selectedDevices = new Set();
let fileQueue = [];        // {file, name, size, id}
let transfers = {};        // session_id → transfer state
let pendingInvite = null;  // current invitation

// ── Init ───────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('my-name').textContent = myName;
    document.getElementById('my-ip').textContent = ` · ${window.location.hostname}`;
    setupTabs();
    setupDropZone();
    connectWS();
    loadFiles();
    setInterval(sendHeartbeat, 5000);
    setInterval(loadFiles, 30000);
});

// ── WebSocket ──────────────────────────────────────────────────
function connectWS() {
    const url = WS_URL + myId;
    log_('Connecting to ' + url, 'info');
    ws = new WebSocket(url);

    ws.onopen = () => {
        setWsStatus(true);
        ws.send(JSON.stringify({
            type: 'register',
            device: {
                device_id: myId,
                name: myName,
                platform: myPlatform,
                ip: window.location.hostname,
                port: window.location.port || 80,
                capabilities: ['send', 'receive', 'sync'],
            }
        }));
        log_('Connected ✓', 'ok');
    };

    ws.onmessage = (e) => {
        try { handleMessage(JSON.parse(e.data)); } catch (err) { console.error(err); }
    };

    ws.onclose = () => {
        setWsStatus(false);
        log_('Disconnected. Reconnecting in 3s…', 'warn');
        setTimeout(connectWS, 3000);
    };

    ws.onerror = () => { log_('WebSocket error', 'error'); };
}

function sendWS(msg) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function sendHeartbeat() {
    sendWS({ type: 'heartbeat' });
}

// ── Message handler ────────────────────────────────────────────
function handleMessage(msg) {
    switch (msg.type) {

        case 'device_list':
            devices = {};
            msg.devices.forEach(d => { if (d.device_id !== myId) devices[d.device_id] = d; });
            renderDevices();
            break;

        case 'device_joined':
            if (msg.device.device_id !== myId) {
                devices[msg.device.device_id] = msg.device;
                renderDevices();
                toast(`${msg.device.name} joined`, 'info');
                log_(`${msg.device.name} joined (${msg.device.platform})`, 'ok');
            }
            break;

        case 'device_left':
            if (devices[msg.device_id]) {
                log_(`${devices[msg.device_id].name} left`, 'warn');
                toast(`${devices[msg.device_id].name} left`, 'warn');
                selectedDevices.delete(msg.device_id);
                delete devices[msg.device_id];
                renderDevices();
                updateSendBar();
            }
            break;

        case 'heartbeat_ack':
            break;

        case 'transfer_invitation':
            pendingInvite = msg;
            showInvitation(msg);
            break;

        case 'transfer_accepted':
            log_(`${getDevName(msg.receiver_id)} accepted transfer ${msg.session_id.slice(0, 8)}`, 'ok');
            updateTransferStatus(msg.session_id, msg.receiver_id, 'sending');
            if (transfers[msg.session_id]) {
                startSendingToDevice(msg.session_id, msg.receiver_id);
            }
            break;

        case 'transfer_rejected':
            log_(`${getDevName(msg.receiver_id)} rejected transfer`, 'warn');
            updateTransferStatus(msg.session_id, msg.receiver_id, 'rejected');
            toast(`${getDevName(msg.receiver_id)} rejected the transfer`, 'warn');
            break;

        case 'transfer_progress':
            updateTransferProgress(msg);
            break;

        case 'file_received':
            log_(`Received: ${msg.filename} (${fmtSize(msg.size)})`, 'ok');
            toast(`Received: ${msg.filename}`, 'ok');
            addTransferEntry({
                id: msg.session_id + '_rx_' + msg.filename,
                name: msg.filename,
                size: msg.size,
                direction: 'received',
                status: 'done',
                progress: 1,
            });
            loadFiles();
            break;

        case 'transfer_complete':
            updateTransferStatus(msg.session_id, msg.receiver_id, 'done');
            break;

        case 'sync_peer_joined':
            log_(`Sync peer joined room "${msg.room}": ${msg.device.name}`, 'data');
            document.getElementById('sync-status').textContent =
                `Room "${msg.room}" · ${msg.device.name} joined`;
            break;

        case 'clipboard_received':
            toast(`📋 Clipboard from ${msg.from}: ${msg.content.slice(0, 40)}`, 'info');
            log_(`Clipboard from ${msg.from}: "${msg.content.slice(0, 60)}"`, 'data');
            break;
    }
}

// ── Device rendering ───────────────────────────────────────────
function renderDevices() {
    const list = document.getElementById('device-list');
    const devArr = Object.values(devices);

    if (devArr.length === 0) {
        list.innerHTML = `<div class="empty" style="padding:30px 14px;font-size:11px">
      <div class="empty-icon">📡</div>Waiting for devices…</div>`;
        return;
    }

    list.innerHTML = devArr.map(d => {
        const sel = selectedDevices.has(d.device_id);
        return `<div class="device-item ${sel ? 'selected' : ''}" onclick="toggleDevice('${d.device_id}')">
      <div class="device-avatar">${platformIcon(d.platform)}</div>
      <div class="device-info">
        <div class="device-name">${esc(d.name)}</div>
        <div class="device-meta">${esc(d.ip || '')} · ${esc(d.platform || '')}</div>
      </div>
      <div class="device-check"></div>
    </div>`;
    }).join('');

    // My own entry at top
    const selfHtml = `<div class="device-item">
    <div class="device-avatar">${platformIcon(myPlatform)}</div>
    <div class="device-info">
      <div class="device-name">${esc(myName)}</div>
      <div class="device-meta">${window.location.hostname}</div>
    </div>
    <div class="self-badge">YOU</div>
  </div>`;
    list.innerHTML = selfHtml + list.innerHTML;
}

function toggleDevice(id) {
    if (selectedDevices.has(id)) selectedDevices.delete(id);
    else selectedDevices.add(id);
    renderDevices();
    updateSendBar();
}

function toggleSelectAll() {
    const devArr = Object.keys(devices);
    if (selectedDevices.size === devArr.length) {
        selectedDevices.clear();
    } else {
        devArr.forEach(id => selectedDevices.add(id));
    }
    renderDevices();
    updateSendBar();
}

// ── Drop zone / file input ─────────────────────────────────────
function setupDropZone() {
    const zone = document.getElementById('drop-zone');
    const input = document.getElementById('file-input');

    zone.addEventListener('click', () => input.click());
    input.addEventListener('change', e => addFiles(e.target.files));

    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
        e.preventDefault();
        zone.classList.remove('drag-over');
        addFiles(e.dataTransfer.files);
    });
}

function addFiles(fileList) {
    Array.from(fileList).forEach(f => {
        fileQueue.push({ file: f, name: f.name, size: f.size, id: generateId() });
    });
    renderQueue();
    updateSendBar();
    log_(`Added ${fileList.length} file(s) to queue`, 'data');
}

function renderQueue() {
    const el = document.getElementById('send-queue');
    const hint = document.getElementById('no-files-hint');
    const bar = document.getElementById('send-bar');

    if (fileQueue.length === 0) {
        el.innerHTML = '';
        bar.style.display = 'none';
        hint.style.display = 'block';
        return;
    }

    hint.style.display = 'none';
    bar.style.display = 'flex';

    el.innerHTML = fileQueue.map(f => `
    <div class="file-item">
      <div class="file-icon">${fileIcon(f.name)}</div>
      <div class="file-meta">
        <div class="file-name">${esc(f.name)}</div>
        <div class="file-size">${fmtSize(f.size)}</div>
      </div>
      <div class="file-remove" onclick="removeFromQueue('${f.id}')">✕</div>
    </div>
  `).join('');
}

function removeFromQueue(id) {
    fileQueue = fileQueue.filter(f => f.id !== id);
    renderQueue();
    updateSendBar();
}

function clearQueue() {
    fileQueue = [];
    renderQueue();
    updateSendBar();
    document.getElementById('file-input').value = '';
}

function updateSendBar() {
    const count = selectedDevices.size;
    const label = document.getElementById('send-targets-label');
    const btn = document.getElementById('send-btn');
    const summary = document.getElementById('send-summary');

    const totalSize = fileQueue.reduce((s, f) => s + f.size, 0);
    summary.textContent = `${fileQueue.length} file(s) · ${fmtSize(totalSize)}`;

    if (count === 0) {
        label.textContent = 'No devices selected';
        btn.disabled = true;
    } else {
        const names = [...selectedDevices].map(id => getDevName(id)).join(', ');
        label.textContent = `→ ${names}`;
        btn.disabled = fileQueue.length === 0;
    }
}

// ── Send ──────────────────────────────────────────────────────
async function sendFiles() {
    if (fileQueue.length === 0 || selectedDevices.size === 0) return;

    const sessionId = generateId();
    const receiverIds = [...selectedDevices];
    const filesMeta = fileQueue.map(f => ({ name: f.name, size: f.size, mime: f.file.type }));

    // Store transfer state
    transfers[sessionId] = {
        sessionId,
        files: [...fileQueue],
        receivers: receiverIds,
        statuses: Object.fromEntries(receiverIds.map(id => [id, 'waiting'])),
        progress: {},
    };

    // Signal receivers
    sendWS({
        type: 'transfer_request',
        session_id: sessionId,
        receiver_ids: receiverIds,
        files: filesMeta,
    });

    // Add transfer entries in UI
    receiverIds.forEach(rid => {
        addTransferEntry({
            id: `${sessionId}_${rid}`,
            sessionId,
            receiverId: rid,
            name: filesMeta.map(f => f.name).join(', '),
            totalSize: filesMeta.reduce((s, f) => s + f.size, 0),
            direction: 'sending',
            status: 'waiting',
            progress: 0,
        });
    });

    switchTab('transfers');
    toast(`Transfer request sent to ${receiverIds.length} device(s)`, 'info');
    log_(`Transfer ${sessionId.slice(0, 8)} → [${receiverIds.map(getDevName).join(', ')}]`, 'data');
}

async function startSendingToDevice(sessionId, receiverId) {
    const state = transfers[sessionId];
    if (!state) return;

    const CHUNK_SIZE = 512 * 1024;

    for (const fObj of state.files) {
        const file = fObj.file;
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
        const uploadId = await initUpload(sessionId, file.name, file.size, totalChunks, receiverId);
        if (!uploadId) {
            log_(`Failed to init upload for ${file.name}`, 'error');
            continue;
        }

        log_(`Uploading ${file.name} → ${getDevName(receiverId)} (${totalChunks} chunks)`, 'data');

        // Upload chunks in parallel batches of 4
        const PARALLEL = 4;
        for (let i = 0; i < totalChunks; i += PARALLEL) {
            const batch = [];
            for (let j = i; j < Math.min(i + PARALLEL, totalChunks); j++) {
                batch.push(uploadChunk(uploadId, j, file, CHUNK_SIZE));
            }
            await Promise.all(batch);

            // Update local progress
            const prog = Math.min((i + PARALLEL) / totalChunks, 1);
            updateLocalProgress(sessionId, receiverId, prog, file.name);
        }

        // Finalize
        const result = await finalizeUpload(uploadId);
        if (result) {
            log_(`${file.name} delivered to ${getDevName(receiverId)} ✓`, 'ok');
        }
    }

    sendWS({ type: 'transfer_complete', session_id: sessionId });
    updateTransferStatus(sessionId, receiverId, 'done');
    toast(`Transfer complete → ${getDevName(receiverId)}`, 'ok');
}

async function initUpload(sessionId, filename, size, totalChunks, receiverId) {
    const fd = new FormData();
    fd.append('session_id', sessionId);
    fd.append('filename', filename);
    fd.append('total_size', size);
    fd.append('total_chunks', totalChunks);
    fd.append('sender_id', myId);
    fd.append('receiver_id', receiverId);

    try {
        const r = await fetch(SERVER + '/api/upload/init', { method: 'POST', body: fd });
        const d = await r.json();
        return d.upload_id;
    } catch (e) {
        log_('Upload init failed: ' + e, 'error');
        return null;
    }
}

async function uploadChunk(uploadId, chunkIndex, file, chunkSize) {
    const start = chunkIndex * chunkSize;
    const blob = file.slice(start, start + chunkSize);
    const fd = new FormData();
    fd.append('upload_id', uploadId);
    fd.append('chunk_index', chunkIndex);
    fd.append('chunk_data', blob, 'chunk');
    try {
        await fetch(SERVER + '/api/upload/chunk', { method: 'POST', body: fd });
    } catch (e) {
        log_(`Chunk ${chunkIndex} failed: ${e}`, 'error');
    }
}

async function finalizeUpload(uploadId) {
    const fd = new FormData();
    fd.append('upload_id', uploadId);
    try {
        const r = await fetch(SERVER + '/api/upload/finalize', { method: 'POST', body: fd });
        return await r.json();
    } catch (e) {
        log_('Finalize failed: ' + e, 'error');
        return null;
    }
}

// ── Invitation ─────────────────────────────────────────────────
function showInvitation(msg) {
    const from = msg.sender?.name || msg.sender?.device_id || 'Unknown';
    document.getElementById('invite-from').textContent =
        `From: ${from} · ${msg.files.length} file(s)`;
    document.getElementById('invite-files').innerHTML = msg.files.map(f =>
        `<div class="modal-file-row">
      <span>${fileIcon(f.name)}</span>
      <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(f.name)}</span>
      <span style="color:var(--text3)">${fmtSize(f.size)}</span>
    </div>`
    ).join('');
    document.getElementById('invite-overlay').classList.remove('hidden');
    log_(`Transfer invitation from ${from}: ${msg.files.map(f => f.name).join(', ')}`, 'info');
}

function acceptTransfer() {
    if (!pendingInvite) return;
    sendWS({ type: 'transfer_accept', session_id: pendingInvite.session_id });
    document.getElementById('invite-overlay').classList.add('hidden');
    toast('Transfer accepted', 'ok');
    log_(`Accepted transfer ${pendingInvite.session_id.slice(0, 8)}`, 'ok');
    pendingInvite = null;
}

function rejectTransfer() {
    if (!pendingInvite) return;
    sendWS({ type: 'transfer_reject', session_id: pendingInvite.session_id });
    document.getElementById('invite-overlay').classList.add('hidden');
    toast('Transfer rejected', 'warn');
    pendingInvite = null;
}

// ── Transfers UI ───────────────────────────────────────────────
let transferEntries = {};

function addTransferEntry(entry) {
    transferEntries[entry.id] = entry;
    renderTransfers();
}

function updateTransferStatus(sessionId, receiverId, status) {
    const id = `${sessionId}_${receiverId}`;
    if (transferEntries[id]) {
        transferEntries[id].status = status;
        if (status === 'done') transferEntries[id].progress = 1;
        renderTransfers();
    }
}

function updateTransferProgress(msg) {
    const id = `${msg.session_id}_${msg.receiver_id || ''}`;
    const entry = transferEntries[id] || Object.values(transferEntries).find(e => e.sessionId === msg.session_id);
    if (entry) {
        entry.progress = msg.progress;
        entry.filename = msg.filename;
        renderTransfers();
    }
}

function updateLocalProgress(sessionId, receiverId, progress, filename) {
    const id = `${sessionId}_${receiverId}`;
    if (transferEntries[id]) {
        transferEntries[id].progress = progress;
        transferEntries[id].status = 'sending';
        transferEntries[id].filename = filename;
        renderTransfers();
    }
}

function renderTransfers() {
    const list = document.getElementById('transfers-list');
    const empty = document.getElementById('transfers-empty');
    const entries = Object.values(transferEntries);
    const badge = document.getElementById('active-badge');

    const active = entries.filter(e => e.status === 'sending' || e.status === 'receiving' || e.status === 'waiting');
    badge.textContent = active.length;
    badge.style.display = active.length > 0 ? 'inline' : 'none';

    if (entries.length === 0) {
        list.innerHTML = '';
        empty.style.display = 'block';
        return;
    }
    empty.style.display = 'none';

    list.innerHTML = entries.slice().reverse().map(e => {
        const pct = Math.round((e.progress || 0) * 100);
        const statusClass = {
            sending: 'status-sending',
            receiving: 'status-receiving',
            done: 'status-done',
            error: 'status-error',
            waiting: 'status-waiting',
            rejected: 'status-error',
        }[e.status] || 'status-waiting';
        const barClass = e.direction === 'received' ? '' : 'blue';
        const target = e.receiverId ? getDevName(e.receiverId) : '';

        return `<div class="transfer-item">
      <div class="transfer-header">
        <div>
          <div class="transfer-name">${fileIcon(e.name)} ${esc(e.filename || e.name)}</div>
          ${target ? `<div style="font-size:11px;color:var(--text2);margin-top:2px;font-family:var(--mono)">${e.direction === 'sending' ? '→' : '←'} ${esc(target)}</div>` : ''}
        </div>
        <div class="transfer-status ${statusClass}">${e.status}</div>
      </div>
      <div class="progress-bar"><div class="progress-fill ${barClass}" style="width:${pct}%"></div></div>
      <div class="transfer-footer">
        <span>${e.totalSize ? fmtSize(e.totalSize) : ''}</span>
        <span>${pct}%</span>
      </div>
    </div>`;
    }).join('');
}

function clearDoneTransfers() {
    Object.keys(transferEntries).forEach(k => {
        if (['done', 'rejected'].includes(transferEntries[k].status)) delete transferEntries[k];
    });
    renderTransfers();
}

// ── Files panel ───────────────────────────────────────────────
async function loadFiles() {
    try {
        const r = await fetch(SERVER + '/api/files');
        const d = await r.json();
        renderFiles(d.files);
    } catch (e) { /* silent */ }
}

function renderFiles(files) {
    const grid = document.getElementById('files-grid');
    const empty = document.getElementById('files-empty');
    const count = document.getElementById('files-count');

    count.textContent = `Received files (${files.length})`;
    if (files.length === 0) {
        grid.innerHTML = '';
        empty.style.display = 'block';
        return;
    }
    empty.style.display = 'none';
    grid.innerHTML = files.map(f => `
    <div class="file-card">
      <div class="file-card-icon">${fileIcon(f.name)}</div>
      <div class="file-card-name">${esc(f.name)}</div>
      <div class="file-card-size">${fmtSize(f.size)}</div>
      <div class="file-card-actions">
        <a href="${SERVER}${f.download_url}" download class="btn btn-ghost" style="font-size:11px;padding:5px 10px">↓ Download</a>
        <button class="btn btn-danger" style="font-size:11px;padding:5px 10px" onclick="deleteFile('${esc(f.name)}')">✕</button>
      </div>
    </div>
  `).join('');
}

async function deleteFile(name) {
    if (!confirm(`Delete "${name}"?`)) return;
    await fetch(`${SERVER}/api/files/${encodeURIComponent(name)}`, { method: 'DELETE' });
    loadFiles();
}

// ── Sync ──────────────────────────────────────────────────────
let inSyncRoom = null;

function joinSyncRoom() {
    const room = document.getElementById('sync-room').value.trim();
    const folder = document.getElementById('sync-folder').value.trim();
    if (!room) return;
    inSyncRoom = room;
    sendWS({ type: 'sync_join', room, folder });
    document.getElementById('sync-status').textContent = `Joined room "${room}" · folder: ${folder}`;
    toast(`Joined sync room: ${room}`, 'ok');
    log_(`Joined sync room "${room}"`, 'ok');
    syncDiff(room, folder);
}

function leaveSyncRoom() {
    inSyncRoom = null;
    document.getElementById('sync-status').textContent = 'Not in a sync room';
}

async function syncDiff(room, folder) {
    try {
        const r = await fetch(SERVER + '/api/sync/diff', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ room, device_id: myId, folder_path: folder, file_tree: [] }),
        });
        const d = await r.json();
        const el = document.getElementById('sync-diff-result');
        const dl = d.need_download.length;
        el.innerHTML = `<div style="font-family:var(--mono);font-size:12px;color:var(--text2);padding:10px 0">
      <div style="color:var(--green)">↓ ${dl} file(s) available to download from server</div>
      ${d.conflicts.length ? `<div style="color:var(--amber);margin-top:4px">⚠ ${d.conflicts.length} conflict(s): ${d.conflicts.join(', ')}</div>` : ''}
    </div>
    ${dl > 0 ? `<div class="files-grid" style="margin-top:10px">` +
                d.need_download.map(f => `<div class="file-card">
        <div class="file-card-icon">${fileIcon(f.name)}</div>
        <div class="file-card-name">${esc(f.name)}</div>
        <div class="file-card-size">${fmtSize(f.size)}</div>
        <div class="file-card-actions">
          <a href="${SERVER}${f.download_url}" download class="btn btn-primary" style="font-size:11px;padding:5px 10px">↓ Sync</a>
        </div>
      </div>`).join('') + `</div>` : ''}`;
    } catch (e) { log_('Sync diff failed: ' + e, 'error'); }
}

// ── Log ──────────────────────────────────────────────────────
function log_(msg, level = 'info') {
    const el = document.getElementById('log-console');
    const ts = new Date().toTimeString().slice(0, 8);
    const line = document.createElement('div');
    line.className = 'log-line';
    line.innerHTML = `<span class="log-ts">${ts}</span><span class="log-msg ${level}">${esc(msg)}</span>`;
    el.appendChild(line);
    el.scrollTop = el.scrollHeight;
}
function clearLog() { document.getElementById('log-console').innerHTML = ''; }

// ── Toasts ────────────────────────────────────────────────────
function toast(msg, type = 'info') {
    const c = document.getElementById('toasts');
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.innerHTML = `<span>${esc(msg)}</span>`;
    c.appendChild(t);
    setTimeout(() => t.remove(), 4000);
}

// ── Tabs ─────────────────────────────────────────────────────
function setupTabs() {
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => switchTab(tab.dataset.tab));
    });
}
function switchTab(name) {
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
    document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id === `tab-${name}`));
    if (name === 'files') loadFiles();
}

// ── WS status ────────────────────────────────────────────────
function setWsStatus(online) {
    const dot = document.getElementById('ws-dot');
    const label = document.getElementById('ws-label');
    dot.classList.toggle('online', online);
    label.textContent = online ? 'connected' : 'offline';
}

// ── Helpers ───────────────────────────────────────────────────
function generateId() {
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
}
function detectName() {
    return `Device_${Math.random().toString(36).slice(2, 6).toUpperCase()}`;
}
function detectPlatform() {
    const ua = navigator.userAgent;
    if (/iPhone|iPad/.test(ua)) return 'iOS';
    if (/Android/.test(ua)) return 'Android';
    if (/Mac/.test(ua)) return 'macOS';
    if (/Win/.test(ua)) return 'Windows';
    if (/Linux/.test(ua)) return 'Linux';
    return 'Browser';
}
function platformIcon(p) {
    return { Windows: '🖥', macOS: '🍎', Linux: '🐧', Android: '🤖', iOS: '📱', Browser: '🌐' }[p] || '💻';
}
function fileIcon(name = '') {
    const ext = name.split('.').pop().toLowerCase();
    const map = {
        pdf: '📄', doc: '📝', docx: '📝', txt: '📃', md: '📃',
        jpg: '🖼', jpeg: '🖼', png: '🖼', gif: '🖼', svg: '🖼', webp: '🖼',
        mp4: '🎬', mov: '🎬', avi: '🎬', mkv: '🎬',
        mp3: '🎵', wav: '🎵', flac: '🎵', aac: '🎵',
        zip: '📦', rar: '📦', gz: '📦', tar: '📦', '7z': '📦',
        exe: '⚙️', dmg: '⚙️', pkg: '⚙️', deb: '⚙️', rpm: '⚙️',
        js: '📜', ts: '📜', py: '🐍', html: '🌐', css: '🎨', json: '📋',
        xls: '📊', xlsx: '📊', csv: '📊',
        ppt: '📊', pptx: '📊',
    };
    return map[ext] || '📁';
}
function fmtSize(bytes) {
    if (!bytes) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;
    while (bytes >= 1024 && i < 4) { bytes /= 1024; i++; }
    return `${bytes.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}
function esc(s = '') {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function getDevName(id) {
    return devices[id]?.name || id.slice(0, 8);
}
