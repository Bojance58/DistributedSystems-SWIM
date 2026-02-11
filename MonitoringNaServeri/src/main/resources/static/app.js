const STATUS_URL = '/cluster/status';
const FIND_URL = '/cluster/find';
const REBALANCE_URL = '/cluster/rebalance';

// primer keys za pokazuvanje na hash ring
const SAMPLE_KEYS = ['cpu:8000', 'cpu:8001', 'cpu:8002', 'cpu:8003'];

function statusClass(state) {
    if (state === 'ALIVE') return 'alive';
    if (state === 'SUSPECT') return 'suspect';
    return 'dead';
}

async function refreshStatus() {
    try {
        const response = await fetch(STATUS_URL);
        const data = await response.json();

        const container = document.getElementById('nodes');
        container.innerHTML = '';

        Object.values(data).forEach(node => {
            const div = document.createElement('div');
            div.className = 'node-card';

            const info = document.createElement('span');
            info.textContent = `${node.id} | hb=${node.heartbeat} | ts=${node.timestamp}`;

            const badge = document.createElement('span');
            badge.className = 'status-badge ' + statusClass(node.state);
            badge.textContent = node.state;

            div.appendChild(info);
            div.appendChild(badge);
            container.appendChild(div);
        });
    } catch (e) {
        console.error('Error loading status', e);
    }
}

async function refreshKeys() {
    const container = document.getElementById('keys');
    container.innerHTML = '';

    for (const key of SAMPLE_KEYS) {
        try {
            const response = await fetch(`${FIND_URL}/${encodeURIComponent(key)}`);
            const text = await response.text();

            const line = document.createElement('div');
            line.className = 'key-line';
            line.textContent = text;
            container.appendChild(line);
        } catch (e) {
            console.error('Error loading key assignment for', key, e);
        }
    }
}

async function rebalanceRing() {
    try {
        const response = await fetch(REBALANCE_URL);
        const text = await response.text();
        alert(text);
        await refreshStatus();
        await refreshKeys();
    } catch (e) {
        console.error('Error rebalancing hash ring', e);
    }
}

// auto-refresh status + keys na sekoe 3 sekundi
setInterval(() => {
    refreshStatus();
    refreshKeys();
}, 3000);

window.onload = () => {
    refreshStatus();
    refreshKeys();
};
