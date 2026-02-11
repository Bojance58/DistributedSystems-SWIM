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

    try {
        const results = await Promise.allSettled(
            SAMPLE_KEYS.map(async (key) => {
                const response = await fetch(`${FIND_URL}/${encodeURIComponent(key)}`);
                const text = await response.text();

                return {
                    key,
                    ok: response.ok,
                    text,
                    status: response.status
                };
            })
        );

        // renderiranje bez "polovicni" sostojbi
        container.innerHTML = '';

        results.forEach((r, i) => {
            const key = SAMPLE_KEYS[i];

            const line = document.createElement('div');
            line.className = 'key-line';

            if (r.status === 'fulfilled') {

                if (r.value.ok) {
                    line.textContent = r.value.text;
                } else {
                    line.textContent = `Key '${key}' -> ERROR ${r.value.status}`;
                }

            } else {
                line.textContent = `Key '${key}' -> FAILED`;
                console.error('Key fetch failed:', key, r.reason);
            }

            container.appendChild(line);
        });

    } catch (e) {
        console.error('Error loading keys', e);
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

async function refreshLoop() {
    await refreshStatus();
    await refreshKeys();
}

// auto refresh na 3 sekundi
setInterval(refreshLoop, 3000);

// initial load
window.onload = refreshLoop;
