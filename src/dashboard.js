const http = require("http");

function toInt(value) {
    if (value === null || value === undefined) return 0;
    const n = Number.parseInt(String(value), 10);
    return Number.isFinite(n) ? n : 0;
}

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function shortDiscordId(id) {
    const s = String(id || "");
    if (s.length <= 8) return s;
    return `${s.slice(0, 4)}...${s.slice(-4)}`;
}

function formatTimestamp(ts) {
    return escapeHtml(String(ts || "").replace("T", " ").slice(0, 19));
}

function formatGold(n) {
    const value = Number(n) || 0;
    if (value >= 1_000_000) {
        const v = value / 1_000_000;
        return `${v % 1 === 0 ? v.toFixed(0) : v.toFixed(2)}M`;
    }
    if (value >= 1_000) {
        const v = value / 1_000;
        return `${v % 1 === 0 ? v.toFixed(0) : v.toFixed(1)}k`;
    }
    return String(value);
}

async function getDashboardData(db, nowISO, getLatestPrice) {
    const [memberCountRes, purchaseCountRes, settingsCountRes, latestPrice, topMembersRes, recentPurchasesRes] = await Promise.all([
        db.query(`SELECT COUNT(*) AS c FROM members`),
        db.query(`SELECT COUNT(*) AS c FROM purchases`),
        db.query(`SELECT COUNT(*) AS c FROM settings`),
        getLatestPrice(),
        db.query(
            `SELECT discord_id, balance_gold, updated_at
             FROM members
             ORDER BY balance_gold DESC, updated_at DESC
             LIMIT 25`
        ),
        db.query(
            `SELECT discord_id, kind, details, gold_cost, balance_after, created_at
             FROM purchases
             ORDER BY id DESC
             LIMIT 15`
        ),
    ]);

    return {
        generatedAt: nowISO(),
        memberCount: toInt(memberCountRes.rows[0]?.c),
        purchaseCount: toInt(purchaseCountRes.rows[0]?.c),
        settingsCount: toInt(settingsCountRes.rows[0]?.c),
        latestPrice,
        topMembers: topMembersRes.rows.map((r) => ({
            discord_id: r.discord_id,
            balance_gold: toInt(r.balance_gold),
            updated_at: r.updated_at,
        })),
        recentPurchases: recentPurchasesRes.rows.map((r) => ({
            discord_id: r.discord_id,
            kind: r.kind,
            details: r.details,
            gold_cost: toInt(r.gold_cost),
            balance_after: toInt(r.balance_after),
            created_at: r.created_at,
        })),
    };
}

function renderDashboardHtml(data) {
    const priceText = data.latestPrice ? `${data.latestPrice.usd_per_1m} USD / 1M` : "Not set";
    const topRows = data.topMembers.map((m, i) => `
      <tr><td>${i + 1}</td><td><code>${escapeHtml(shortDiscordId(m.discord_id))}</code></td><td>${escapeHtml(formatGold(m.balance_gold))}</td><td><small>${formatTimestamp(m.updated_at)}</small></td></tr>`).join("");
    const purchaseRows = data.recentPurchases.map((p) => `
      <tr><td><code>${escapeHtml(shortDiscordId(p.discord_id))}</code></td><td>${escapeHtml(String(p.kind || "").toUpperCase())}</td><td title="${escapeHtml(p.details)}">${escapeHtml(String(p.details || "").slice(0, 32))}</td><td>-${escapeHtml(formatGold(p.gold_cost))}</td><td>${escapeHtml(formatGold(p.balance_after))}</td><td><small>${formatTimestamp(p.created_at)}</small></td></tr>`).join("");

    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Balance Bot Dashboard</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--line:#2a3340;--txt:#e6edf3;--muted:#9fb0c3;--acc:#4c8dff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(900px 500px at 90% -10%,#1a2a50,transparent 60%),radial-gradient(800px 500px at -10% 10%,#15352f,transparent 60%),var(--bg);color:var(--txt);font-family:Segoe UI,Arial,sans-serif}
.wrap{max-width:1200px;margin:0 auto;padding:18px}.hero{display:flex;justify-content:space-between;gap:12px;align-items:end}.hero h1{margin:0}.hero p{margin:4px 0 0;color:var(--muted)}
.pill{padding:8px 10px;border-radius:999px;border:1px solid rgba(76,141,255,.4);background:rgba(76,141,255,.12);font-size:12px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:14px 0}.card{background:linear-gradient(180deg,var(--panel),#1b2230);border:1px solid var(--line);border-radius:14px;padding:14px}.k{color:var(--muted);font-size:12px}.v{font-size:24px;font-weight:700;margin-top:6px}.sub{color:var(--muted);font-size:12px;margin-top:6px}
.split{display:grid;grid-template-columns:1.05fr .95fr;gap:12px}.panel{background:linear-gradient(180deg,var(--panel),#151c26);border:1px solid var(--line);border-radius:14px;padding:12px}.panel h2{margin:0 0 10px;font-size:16px}
table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid var(--line);padding:8px 6px;text-align:left;font-size:13px;vertical-align:top}th{color:var(--muted)}code{background:rgba(76,141,255,.10);padding:2px 4px;border-radius:4px}
.links{margin-top:8px}.links a{color:var(--acc);text-decoration:none}.links a:hover{text-decoration:underline}
@media (max-width:900px){.cards{grid-template-columns:1fr 1fr}.split{grid-template-columns:1fr}}@media (max-width:560px){.cards{grid-template-columns:1fr}.hero{flex-direction:column;align-items:start}}
</style></head><body>
<div class="wrap"><div class="hero"><div><h1>Balance Bot Dashboard</h1><p>Web view for balances, purchases, and price status.</p></div><div class="pill">${formatTimestamp(data.generatedAt)} UTC</div></div>
<div class="cards">
<div class="card"><div class="k">Members</div><div class="v">${data.memberCount}</div><div class="sub">Tracked users</div></div>
<div class="card"><div class="k">Purchases</div><div class="v">${data.purchaseCount}</div><div class="sub">Recorded purchases</div></div>
<div class="card"><div class="k">Guild Settings</div><div class="v">${data.settingsCount}</div><div class="sub">Configured guild rows</div></div>
<div class="card"><div class="k">Latest Price</div><div class="v" style="font-size:18px">${escapeHtml(priceText)}</div><div class="sub">${data.latestPrice ? formatTimestamp(data.latestPrice.updated_at) : "No price set"}</div></div>
</div>
<div class="split">
<section class="panel"><h2>Top Balances</h2><table><thead><tr><th>#</th><th>User</th><th>Balance</th><th>Updated</th></tr></thead><tbody>${topRows || '<tr><td colspan="4">No member records.</td></tr>'}</tbody></table><div class="links"><a href="/api/overview" target="_blank" rel="noreferrer">Open JSON API</a></div></section>
<section class="panel"><h2>Recent Purchases</h2><table><thead><tr><th>User</th><th>Kind</th><th>Details</th><th>Cost</th><th>After</th><th>Time</th></tr></thead><tbody>${purchaseRows || '<tr><td colspan="6">No purchases.</td></tr>'}</tbody></table></section>
</div></div></body></html>`;
}

function startDashboardServer({ db, nowISO, getLatestPrice, port }) {
    const listenPort = Number(port || process.env.DASHBOARD_PORT || process.env.PORT || 3000);
    const server = http.createServer(async (req, res) => {
        try {
            const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
            if (url.pathname === "/health") {
                res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
                res.end(JSON.stringify({ ok: true, service: "balance-bot-dashboard" }));
                return;
            }

            const data = await getDashboardData(db, nowISO, getLatestPrice);

            if (url.pathname === "/api/overview") {
                res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
                res.end(JSON.stringify(data));
                return;
            }
            if (url.pathname !== "/") {
                res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
                res.end("Not found");
                return;
            }

            res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
            res.end(renderDashboardHtml(data));
        } catch (err) {
            console.error("Dashboard request error:", err);
            res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
            res.end("Dashboard error");
        }
    });

    server.listen(listenPort, () => {
        console.log(`Dashboard running at http://localhost:${listenPort}`);
    });

    return server;
}

module.exports = { startDashboardServer };
