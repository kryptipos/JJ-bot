const http = require("http");
const crypto = require("crypto");

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

function formatPrettyTimestamp(ts) {
    if (!ts) return "-";
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return formatTimestamp(ts);
    return escapeHtml(
        d.toLocaleString("en-GB", {
            day: "2-digit",
            month: "short",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
        })
    );
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

function parseCookies(req) {
    const raw = req.headers.cookie || "";
    const out = {};
    for (const part of raw.split(";")) {
        const [k, ...rest] = part.trim().split("=");
        if (!k) continue;
        out[k] = decodeURIComponent(rest.join("=") || "");
    }
    return out;
}

function sendRedirect(res, location, cookieHeaders = []) {
    const headers = { Location: location };
    if (cookieHeaders.length) headers["Set-Cookie"] = cookieHeaders;
    res.writeHead(302, headers);
    res.end();
}

function getOAuthConfig() {
    const clientId = process.env.CLIENT_ID;
    const clientSecret = process.env.CLIENT_SECRET || process.env.DISCORD_CLIENT_SECRET;
    const baseUrl = process.env.DASHBOARD_BASE_URL;
    return {
        clientId,
        clientSecret,
        baseUrl,
        enabled: Boolean(clientId && clientSecret && baseUrl),
    };
}

function getAdminIdSet() {
    const raw = process.env.DASHBOARD_ADMIN_IDS || "";
    return new Set(
        raw
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean)
    );
}

function getDashboardLogoUrl(fallback = null) {
    return process.env.DASHBOARD_LOGO_URL || fallback || null;
}

function isAdminDiscordId(discordId) {
    if (!discordId) return false;
    return getAdminIdSet().has(String(discordId));
}

function buildDiscordOAuthUrl(state) {
    const { clientId, baseUrl } = getOAuthConfig();
    const redirectUri = `${baseUrl.replace(/\/$/, "")}/auth/callback`;
    const qs = new URLSearchParams({
        client_id: clientId,
        response_type: "code",
        redirect_uri: redirectUri,
        scope: "identify",
        prompt: "none",
        state,
    });
    return `https://discord.com/api/oauth2/authorize?${qs.toString()}`;
}

async function exchangeCodeForToken(code) {
    const { clientId, clientSecret, baseUrl } = getOAuthConfig();
    const redirectUri = `${baseUrl.replace(/\/$/, "")}/auth/callback`;
    const body = new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        grant_type: "authorization_code",
        code,
        redirect_uri: redirectUri,
    });

    const resp = await fetch("https://discord.com/api/oauth2/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
    });
    if (!resp.ok) throw new Error(`oauth_token_${resp.status}`);
    return resp.json();
}

async function fetchDiscordUser(accessToken) {
    const resp = await fetch("https://discord.com/api/users/@me", {
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!resp.ok) throw new Error(`oauth_user_${resp.status}`);
    return resp.json();
}

async function getUserDashboardData(db, client, discordId) {
    const [memberRes, purchasesRes, totalSpentRes] = await Promise.all([
        db.query(`SELECT balance_gold, updated_at FROM members WHERE discord_id = $1`, [discordId]),
        db.query(
            `SELECT kind, details, gold_cost, balance_after, created_at
             FROM purchases
             WHERE discord_id = $1
             ORDER BY id DESC
             LIMIT 25`,
            [discordId]
        ),
        db.query(`SELECT COALESCE(SUM(gold_cost), 0) AS total_gold FROM purchases WHERE discord_id = $1`, [discordId]),
    ]);

    let user = client?.users?.cache?.get(discordId) || null;
    if (!user && client) user = await client.users.fetch(discordId).catch(() => null);

    let guildRole = null;
    let guildIconUrl = null;
    const guildId = process.env.GUILD_ID;
    if (guildId && client?.guilds) {
        const guild = client.guilds.cache.get(guildId) || await client.guilds.fetch(guildId).catch(() => null);
        if (guild?.iconURL) {
            guildIconUrl = guild.iconURL({ extension: "png", size: 128 }) || null;
        }
        const member = guild ? await guild.members.fetch(discordId).catch(() => null) : null;
        if (member) {
            const roleNames = member.roles.cache.map((r) => r.name);
            guildRole = ["Legendary", "Epic", "Rare", "Common"].find((n) => roleNames.includes(n)) || null;
        }
    }

    return {
        discordId,
        isAdmin: isAdminDiscordId(discordId),
        username: user?.username || null,
        avatarUrl: user?.displayAvatarURL ? user.displayAvatarURL({ extension: "png", size: 128 }) : null,
        guildIconUrl,
        guildRole,
        totalSpentGold: toInt(totalSpentRes.rows[0]?.total_gold),
        member: memberRes.rows[0]
            ? {
                  balance_gold: toInt(memberRes.rows[0].balance_gold),
                  updated_at: memberRes.rows[0].updated_at,
              }
            : null,
        purchases: purchasesRes.rows.map((r) => ({
            kind: r.kind,
            details: r.details,
            gold_cost: toInt(r.gold_cost),
            balance_after: toInt(r.balance_after),
            created_at: r.created_at,
        })),
    };
}

async function getDiscordOrderChannelUrl(db) {
    const guildId = process.env.GUILD_ID;
    let row = null;
    if (guildId) {
        const res = await db.query(`SELECT guild_id, order_channel_id FROM settings WHERE guild_id = $1`, [guildId]);
        row = res.rows[0] || null;
    }
    if (!row) {
        const res = await db.query(`SELECT guild_id, order_channel_id FROM settings ORDER BY guild_id LIMIT 1`);
        row = res.rows[0] || null;
    }
    if (!row?.guild_id || !row?.order_channel_id) return null;
    return `https://discord.com/channels/${row.guild_id}/${row.order_channel_id}`;
}

function getDiscordServerUrl() {
    const guildId = process.env.GUILD_ID;
    if (!guildId) return "https://discord.com/app";
    return `https://discord.com/channels/${guildId}`;
}

function renderUserDashboardHtml(data) {
    const rows = data.purchases.map((p) => `
      <tr><td>${escapeHtml(String(p.kind || "").toUpperCase())}</td><td title="${escapeHtml(p.details)}">${escapeHtml(String(p.details || "").slice(0, 40))}</td><td>-${escapeHtml(formatGold(p.gold_cost))}</td><td>${escapeHtml(formatGold(p.balance_after))}</td><td>${formatTimestamp(p.created_at)}</td></tr>`).join("");
    const userLabel = data.username || `User ${shortDiscordId(data.discordId)}`;
    const adminLink = data.isAdmin
        ? `<div style="margin-top:10px"><a href="/admin" style="display:inline-block;padding:8px 12px;border-radius:10px;background:#1a2433;border:1px solid #2a3340;color:#e6edf3;text-decoration:none">Go to Admin Dashboard</a></div>`
        : "";
    const tierColor = ({
        Legendary: "#f39c12",
        Epic: "#9b59b6",
        Rare: "#3498db",
        Common: "#95a5a6",
    })[data.guildRole || "Common"] || "#95a5a6";
    const tierTheme = ({
        Legendary: {
            glowA: "rgba(243,156,18,.22)",
            glowB: "rgba(231,76,60,.14)",
            bg1: "#1b1510",
            bg2: "#121018",
        },
        Epic: {
            glowA: "rgba(155,89,182,.24)",
            glowB: "rgba(76,141,255,.12)",
            bg1: "#171224",
            bg2: "#10131d",
        },
        Rare: {
            glowA: "rgba(52,152,219,.24)",
            glowB: "rgba(47,196,166,.12)",
            bg1: "#111a26",
            bg2: "#0f151f",
        },
        Common: {
            glowA: "rgba(149,165,166,.18)",
            glowB: "rgba(76,141,255,.10)",
            bg1: "#131922",
            bg2: "#10161f",
        },
    })[data.guildRole || "Common"] || {
        glowA: "rgba(149,165,166,.18)",
        glowB: "rgba(76,141,255,.10)",
        bg1: "#131922",
        bg2: "#10161f",
    };
    const faviconUrl = getDashboardLogoUrl(data.guildIconUrl || data.avatarUrl);
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>My Dashboard</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>
body{margin:0;background:#0d1117;color:#e6edf3;font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:980px;margin:0 auto;padding:20px}
.top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:10px}.panel{background:#161b22;border:1px solid #2a3340;border-radius:14px;padding:14px;margin-top:14px}
.hero{display:flex;gap:10px;align-items:center}.hero img{width:40px;height:40px;border-radius:50%;border:2px solid #2a3340}.muted{color:#9fb0c3}
.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.card{background:#141a22;border:1px solid #2a3340;border-radius:12px;padding:12px}.k{color:#9fb0c3;font-size:12px}.v{font-size:22px;font-weight:700;margin-top:6px}
.member-card{margin-top:14px;border-radius:18px;padding:16px;border:1px solid #2a3340;background:
 radial-gradient(520px 240px at 90% -10%, ${tierTheme.glowA}, transparent 62%),
 radial-gradient(420px 220px at 0% 100%, ${tierTheme.glowB}, transparent 60%),
 linear-gradient(180deg,${tierTheme.bg1},${tierTheme.bg2}); box-shadow: inset 0 1px 0 rgba(255,255,255,.03), 0 12px 30px rgba(0,0,0,.18);}
.member-card-grid{display:grid;grid-template-columns:120px 1fr;gap:16px;align-items:center}
.avatar-big{width:108px;height:108px;border-radius:50%;border:4px solid ${tierColor};object-fit:cover;background:#0b0f14}
.tier-pill{display:inline-block;padding:6px 10px;border-radius:999px;border:1px solid ${tierColor};color:${tierColor};background:color-mix(in srgb, ${tierColor} 10%, transparent);font-weight:700;font-size:12px;box-shadow:0 0 0 1px rgba(255,255,255,.02) inset}
.member-stats{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:12px}
.member-stat{background:#0f141b;border:1px solid #263140;border-radius:12px;padding:10px}
.meta-chip{display:inline-flex;align-items:center;gap:8px;margin-top:10px;padding:7px 10px;border-radius:999px;border:1px solid #2a3340;background:linear-gradient(180deg,#111723,#0e141d);color:#b8c7d8;font-size:12px}
.meta-chip strong{color:#d8e6f5;font-weight:700}
table{width:100%;border-collapse:collapse}th,td{padding:8px 6px;border-bottom:1px solid #2a3340;text-align:left;font-size:13px}th{color:#9fb0c3}
a{color:#68a3ff;text-decoration:none}a:hover{text-decoration:underline}
@media(max-width:700px){.cards,.member-stats{grid-template-columns:1fr}.top{flex-direction:column;align-items:start}.member-card-grid{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<div class="top"><div class="hero">${(data.guildIconUrl || data.avatarUrl) ? `<img src="${escapeHtml(data.guildIconUrl || data.avatarUrl)}" alt="server logo"/>` : ""}<div><h1 style="margin:0;font-size:18px">My Dashboard</h1>${adminLink}</div></div><div><a href="/logout">Logout</a></div></div>
<section class="member-card">
  <div class="member-card-grid">
    <div>${data.avatarUrl ? `<img class="avatar-big" src="${escapeHtml(data.avatarUrl)}" alt="avatar"/>` : `<div class="avatar-big"></div>`}</div>
    <div>
      <div class="tier-pill">${escapeHtml((data.guildRole || "Member") + " Tier")}</div>
      <h2 style="margin:10px 0 4px;font-size:26px">${escapeHtml(userLabel)}</h2>
      <div class="muted">Member Card</div>
      <div class="member-stats">
        <div class="member-stat"><div class="k">Balance</div><div class="v" style="font-size:18px">${data.member ? escapeHtml(formatGold(data.member.balance_gold)) : "No Record"}</div></div>
        <div class="member-stat"><div class="k">Total Spent</div><div class="v" style="font-size:18px">${escapeHtml(formatGold(data.totalSpentGold || 0))}</div></div>
        <div class="member-stat"><div class="k">Purchases</div><div class="v" style="font-size:18px">${data.purchases.length}</div></div>
      </div>
      <div class="meta-chip"><strong>Last Updated</strong><span>${data.member ? formatPrettyTimestamp(data.member.updated_at) : "-"}</span></div>
    </div>
  </div>
</section>
<section class="panel">
  <h2 style="margin:0 0 10px">Actions</h2>
  <div style="display:flex;gap:10px;flex-wrap:wrap">
    <a href="/me/order" style="display:inline-block;padding:10px 14px;border-radius:10px;background:#1e2816;border:1px solid #304224;color:#e6edf3;text-decoration:none">Order Gold / Boost</a>
  </div>
</section>
<section class="panel"><h2 style="margin:0 0 10px">Your Recent Purchases</h2>
<table><thead><tr><th>Kind</th><th>Details</th><th>Cost</th><th>After</th><th>Time</th></tr></thead><tbody>${rows || '<tr><td colspan="5">No purchases yet.</td></tr>'}</tbody></table>
</section></div></body></html>`;
}

function renderAccessDeniedHtml() {
    const faviconUrl = getDashboardLogoUrl();
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Access Denied</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>body{margin:0;background:#0d1117;color:#e6edf3;font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:720px;margin:80px auto;padding:20px}.card{background:#161b22;border:1px solid #2a3340;border-radius:14px;padding:18px}a{color:#68a3ff;text-decoration:none}a:hover{text-decoration:underline}</style>
</head><body><div class="wrap"><div class="card"><h1 style="margin-top:0">Access Denied</h1><p>You are logged in, but you do not have admin access to the global dashboard.</p><p><a href="/me">Go to My Dashboard</a></p></div></div></body></html>`;
}

function renderSimpleActionPage({ title, message, backHref = "/me", primaryLinkHref = null, primaryLinkLabel = null }) {
    const faviconUrl = getDashboardLogoUrl();
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>${escapeHtml(title)}</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>body{margin:0;background:#0d1117;color:#e6edf3;font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:760px;margin:60px auto;padding:20px}.card{background:#161b22;border:1px solid #2a3340;border-radius:14px;padding:18px}a{color:#68a3ff;text-decoration:none}a:hover{text-decoration:underline}</style>
</head><body><div class="wrap"><div class="card"><h1 style="margin-top:0">${escapeHtml(title)}</h1><p>${escapeHtml(message)}</p>${primaryLinkHref ? `<p><a href="${escapeHtml(primaryLinkHref)}" target="_blank" rel="noreferrer">${escapeHtml(primaryLinkLabel || "Open Discord")}</a></p>` : ""}<p><a href="${escapeHtml(backHref)}">Back</a></p></div></div></body></html>`;
}

async function resolveUserLabels(client, ids) {
    const uniqueIds = [...new Set(ids.filter(Boolean).map(String))];
    const labels = new Map();
    if (!client) return labels;

    for (const id of uniqueIds) {
        try {
            const cached = client.users?.cache?.get(id);
            if (cached) {
                labels.set(id, cached.username || cached.tag || id);
                continue;
            }
            const user = await client.users.fetch(id).catch(() => null);
            if (user) {
                labels.set(id, user.username || user.tag || id);
            }
        } catch {
            // ignore lookup failures; UI will fall back to ID
        }
    }
    return labels;
}

async function getDashboardData(db, nowISO, getLatestPrice, client) {
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

    const allIds = [
        ...topMembersRes.rows.map((r) => r.discord_id),
        ...recentPurchasesRes.rows.map((r) => r.discord_id),
    ];
    const userLabels = await resolveUserLabels(client, allIds);
    let guildIconUrl = null;
    const guildId = process.env.GUILD_ID;
    if (guildId && client?.guilds) {
        const guild = client.guilds.cache.get(guildId) || await client.guilds.fetch(guildId).catch(() => null);
        if (guild?.iconURL) guildIconUrl = guild.iconURL({ extension: "png", size: 128 }) || null;
    }

    return {
        generatedAt: nowISO(),
        guildIconUrl,
        memberCount: toInt(memberCountRes.rows[0]?.c),
        purchaseCount: toInt(purchaseCountRes.rows[0]?.c),
        settingsCount: toInt(settingsCountRes.rows[0]?.c),
        latestPrice,
        topMembers: topMembersRes.rows.map((r) => ({
            discord_id: r.discord_id,
            user_label: userLabels.get(String(r.discord_id)) || null,
            balance_gold: toInt(r.balance_gold),
            updated_at: r.updated_at,
        })),
        recentPurchases: recentPurchasesRes.rows.map((r) => ({
            discord_id: r.discord_id,
            user_label: userLabels.get(String(r.discord_id)) || null,
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
      <tr><td>${i + 1}</td><td>${escapeHtml(m.user_label || "Unknown")}<br/><small><code>${escapeHtml(shortDiscordId(m.discord_id))}</code></small></td><td>${escapeHtml(formatGold(m.balance_gold))}</td><td><small>${formatTimestamp(m.updated_at)}</small></td></tr>`).join("");
    const purchaseRows = data.recentPurchases.map((p) => `
      <tr><td>${escapeHtml(p.user_label || "Unknown")}<br/><small><code>${escapeHtml(shortDiscordId(p.discord_id))}</code></small></td><td>${escapeHtml(String(p.kind || "").toUpperCase())}</td><td title="${escapeHtml(p.details)}">${escapeHtml(String(p.details || "").slice(0, 32))}</td><td>-${escapeHtml(formatGold(p.gold_cost))}</td><td>${escapeHtml(formatGold(p.balance_after))}</td><td><small>${formatTimestamp(p.created_at)}</small></td></tr>`).join("");

    const brandLogo = getDashboardLogoUrl(data.guildIconUrl || null);
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Balance Bot Dashboard</title>${brandLogo ? `<link rel="icon" href="${escapeHtml(brandLogo)}"/>` : ""}
<style>
:root{--bg:#0d1117;--panel:#161b22;--line:#2a3340;--txt:#e6edf3;--muted:#9fb0c3;--acc:#4c8dff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(900px 500px at 90% -10%,#1a2a50,transparent 60%),radial-gradient(800px 500px at -10% 10%,#15352f,transparent 60%),var(--bg);color:var(--txt);font-family:Segoe UI,Arial,sans-serif}
  .wrap{max-width:1200px;margin:0 auto;padding:18px}.hero{display:flex;justify-content:space-between;gap:12px;align-items:end}.hero h1{margin:0}.hero p{margin:4px 0 0;color:var(--muted)}.brand{display:flex;gap:12px;align-items:center}.brand img{width:42px;height:42px;border-radius:50%;border:2px solid var(--line);object-fit:cover}
.pill{padding:8px 10px;border-radius:999px;border:1px solid rgba(76,141,255,.4);background:rgba(76,141,255,.12);font-size:12px}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:14px 0}.card{background:linear-gradient(180deg,var(--panel),#1b2230);border:1px solid var(--line);border-radius:14px;padding:14px}.k{color:var(--muted);font-size:12px}.v{font-size:24px;font-weight:700;margin-top:6px}.sub{color:var(--muted);font-size:12px;margin-top:6px}
.split{display:grid;grid-template-columns:1.05fr .95fr;gap:12px}.panel{background:linear-gradient(180deg,var(--panel),#151c26);border:1px solid var(--line);border-radius:14px;padding:12px}.panel h2{margin:0 0 10px;font-size:16px}
table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid var(--line);padding:8px 6px;text-align:left;font-size:13px;vertical-align:top}th{color:var(--muted)}code{background:rgba(76,141,255,.10);padding:2px 4px;border-radius:4px}
.links{margin-top:8px}.links a{color:var(--acc);text-decoration:none}.links a:hover{text-decoration:underline}
@media (max-width:900px){.cards{grid-template-columns:1fr 1fr}.split{grid-template-columns:1fr}}@media (max-width:560px){.cards{grid-template-columns:1fr}.hero{flex-direction:column;align-items:start}}
</style></head><body>
  <div class="wrap"><div class="hero"><div class="brand">${brandLogo ? `<img src="${escapeHtml(brandLogo)}" alt="logo"/>` : ""}<div><h1>Balance Bot Dashboard</h1><p>Web view for balances, purchases, and price status.</p></div></div><div class="pill">${formatTimestamp(data.generatedAt)} UTC</div></div>
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

function startDashboardServer({ db, nowISO, getLatestPrice, client, port }) {
    const listenPort = Number(port || process.env.DASHBOARD_PORT || process.env.PORT || 3000);
    const sessions = new Map();
    const oauthStates = new Map();
    const server = http.createServer(async (req, res) => {
        try {
            const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
            const cookies = parseCookies(req);
            const sessionId = cookies.jj_dash_session;
            const session = sessionId ? sessions.get(sessionId) : null;

            if (url.pathname === "/health") {
                res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
                res.end(JSON.stringify({ ok: true, service: "balance-bot-dashboard" }));
                return;
            }

            if (url.pathname === "/login") {
                const cfg = getOAuthConfig();
                if (!cfg.enabled) {
                    res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
                    res.end("Discord OAuth not configured. Set CLIENT_ID, CLIENT_SECRET, DASHBOARD_BASE_URL.");
                    return;
                }
                const state = crypto.randomBytes(16).toString("hex");
                const next = url.searchParams.get("next") || "/me";
                oauthStates.set(state, { createdAt: Date.now(), next });
                sendRedirect(res, buildDiscordOAuthUrl(state));
                return;
            }

            if (url.pathname === "/auth/callback") {
                const code = url.searchParams.get("code");
                const state = url.searchParams.get("state");
                if (!code || !state || !oauthStates.has(state)) {
                    res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
                    res.end("Invalid OAuth callback.");
                    return;
                }
                const oauthState = oauthStates.get(state);
                oauthStates.delete(state);
                const token = await exchangeCodeForToken(code);
                const user = await fetchDiscordUser(token.access_token);
                const newSessionId = crypto.randomBytes(24).toString("hex");
                sessions.set(newSessionId, {
                    discordId: String(user.id),
                    username: user.username || null,
                    createdAt: Date.now(),
                });
                sendRedirect(
                    res,
                    oauthState?.next || "/me",
                    [`jj_dash_session=${encodeURIComponent(newSessionId)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800`]
                );
                return;
            }

            if (url.pathname === "/logout") {
                if (sessionId) sessions.delete(sessionId);
                sendRedirect(res, "/", ["jj_dash_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"]);
                return;
            }

            if (url.pathname === "/me") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login?next=%2Fme");
                    return;
                }
                const data = await getUserDashboardData(db, client, session.discordId);
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderUserDashboardHtml(data));
                return;
            }

            if (url.pathname === "/me/order") {
                if (!session?.discordId) {
                    sendRedirect(res, `/login?next=${encodeURIComponent(url.pathname)}`);
                    return;
                }
                const orderUrl = await getDiscordOrderChannelUrl(db);
                if (orderUrl) {
                    sendRedirect(res, orderUrl);
                    return;
                }
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(
                    renderSimpleActionPage({
                        title: "Order Gold / Boost",
                        message: "Your order channel is not configured yet in the dashboard backend. Open Discord and use the bot buttons in your server.",
                        primaryLinkHref: getDiscordServerUrl(),
                        primaryLinkLabel: "Open Discord Server",
                    })
                );
                return;
            }

            if (url.pathname === "/admin") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login?next=%2Fadmin");
                    return;
                }
                if (!isAdminDiscordId(session.discordId)) {
                    res.writeHead(403, { "Content-Type": "text/html; charset=utf-8" });
                    res.end(renderAccessDeniedHtml());
                    return;
                }
                const data = await getDashboardData(db, nowISO, getLatestPrice, client);
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderDashboardHtml(data));
                return;
            }

            if (url.pathname === "/") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login");
                    return;
                }
                if (isAdminDiscordId(session.discordId)) {
                    sendRedirect(res, "/admin");
                } else {
                    sendRedirect(res, "/me");
                }
                return;
            }

            const data = await getDashboardData(db, nowISO, getLatestPrice, client);

            if (url.pathname === "/api/overview") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login");
                    return;
                }
                if (!isAdminDiscordId(session.discordId)) {
                    res.writeHead(403, { "Content-Type": "application/json; charset=utf-8" });
                    res.end(JSON.stringify({ error: "forbidden" }));
                    return;
                }
                res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
                res.end(JSON.stringify(data));
                return;
            }
            if (url.pathname !== "/") {
                res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
                res.end("Not found");
                return;
            }
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
