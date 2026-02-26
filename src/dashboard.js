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

function hasManageGuildPermission(member) {
    if (!member) return false;
    try {
        if (member.permissions?.has?.("Administrator") || member.permissions?.has?.("ManageGuild")) return true;
    } catch {}
    try {
        const perms = member.permissions;
        if (perms?.has?.(8n) || perms?.has?.(0x20n)) return true;
    } catch {}
    return false;
}

function hasManageGuildPermissionBits(rawPermissions, isOwner = false) {
    if (isOwner) return true;
    try {
        const perms = BigInt(String(rawPermissions || "0"));
        return Boolean((perms & 0x8n) === 0x8n || (perms & 0x20n) === 0x20n);
    } catch {
        return false;
    }
}

function buildDiscordOAuthUrl(state) {
    const { clientId, baseUrl } = getOAuthConfig();
    const redirectUri = `${baseUrl.replace(/\/$/, "")}/auth/callback`;
    const qs = new URLSearchParams({
        client_id: clientId,
        response_type: "code",
        redirect_uri: redirectUri,
        scope: "identify guilds",
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

async function fetchDiscordUserGuilds(accessToken) {
    const resp = await fetch("https://discord.com/api/users/@me/guilds", {
        headers: { Authorization: `Bearer ${accessToken}` },
    });
    if (!resp.ok) throw new Error(`oauth_guilds_${resp.status}`);
    return resp.json();
}

async function getUserDashboardData(db, client, discordId, selectedGuildId = null) {
    const guildId = selectedGuildId || process.env.GUILD_ID || null;
    const [memberRes, purchasesRes, totalSpentRes] = await Promise.all([
        guildId
            ? db.query(`SELECT balance_gold, updated_at FROM members WHERE guild_id = $1 AND discord_id = $2`, [guildId, discordId])
            : db.query(`SELECT balance_gold, updated_at FROM members WHERE discord_id = $1`, [discordId]),
        db.query(
            `SELECT kind, details, gold_cost, balance_after, created_at
             FROM purchases
             WHERE ${guildId ? "guild_id = $1 AND discord_id = $2" : "discord_id = $1"}
             ORDER BY id DESC
             LIMIT 25`,
            guildId ? [guildId, discordId] : [discordId]
        ),
        guildId
            ? db.query(`SELECT COALESCE(SUM(gold_cost), 0) AS total_gold FROM purchases WHERE guild_id = $1 AND discord_id = $2`, [guildId, discordId])
            : db.query(`SELECT COALESCE(SUM(gold_cost), 0) AS total_gold FROM purchases WHERE discord_id = $1`, [discordId]),
    ]);

    let user = client?.users?.cache?.get(discordId) || null;
    if (!user && client) user = await client.users.fetch(discordId).catch(() => null);

    let guildRole = null;
    let guildIconUrl = null;
    if (guildId && client?.guilds) {
        const guild = client.guilds.cache.get(guildId) || await client.guilds.fetch(guildId).catch(() => null);
        if (guild?.iconURL) {
            guildIconUrl = guild.iconURL({ extension: "png", size: 128 }) || null;
        }
        const member = guild
            ? await guild.members.fetch({ user: discordId, force: true }).catch(() => null)
            : null;
        if (member) {
            const roleNames = member.roles.cache.map((r) => String(r.name || ""));
            const normalizedRoles = roleNames.map((n) => n.toLowerCase());
            const tierOrder = ["legendary", "epic", "rare", "common"];
            const foundTier = tierOrder.find((tier) =>
                normalizedRoles.some((roleName) => roleName === tier || roleName.includes(tier))
            );
            guildRole = foundTier ? foundTier.charAt(0).toUpperCase() + foundTier.slice(1) : null;
        }
    }

    return {
        discordId,
        selectedGuildId: guildId,
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

async function getManageableGuilds(db, client, discordId) {
    const guildIds = new Set();
    for (const table of ["settings", "members", "purchases"]) {
        const col = "guild_id";
        const res = await db.query(`SELECT DISTINCT ${col} FROM ${table} WHERE ${col} IS NOT NULL`);
        for (const row of res.rows) guildIds.add(String(row.guild_id));
    }

    const out = [];
    for (const guildId of guildIds) {
        const guild = client?.guilds?.cache?.get(guildId) || await client?.guilds?.fetch?.(guildId).catch(() => null);
        if (!guild) continue;
        const member = await guild.members.fetch({ user: discordId, force: true }).catch(() => null);
        if (!hasManageGuildPermission(member)) continue;
        out.push({
            guildId,
            name: guild.name || `Guild ${guildId}`,
            iconUrl: guild.iconURL ? guild.iconURL({ extension: "png", size: 128 }) : null,
        });
    }
    out.sort((a, b) => a.name.localeCompare(b.name));
    return out;
}

async function getUserDataGuilds(db, client, discordId) {
    const res = await db.query(
        `SELECT guild_id, MAX(last_seen) AS last_seen
         FROM (
           SELECT guild_id, updated_at::text AS last_seen FROM members WHERE discord_id = $1
           UNION ALL
           SELECT guild_id, created_at::text AS last_seen FROM purchases WHERE discord_id = $1
         ) t
         GROUP BY guild_id
         ORDER BY MAX(last_seen) DESC`,
        [discordId]
    );

    const out = [];
    for (const row of res.rows) {
        const guildId = String(row.guild_id);
        const guild = client?.guilds?.cache?.get(guildId) || await client?.guilds?.fetch?.(guildId).catch(() => null);
        out.push({
            guildId,
            name: guild?.name || `Guild ${guildId}`,
            iconUrl: guild?.iconURL ? guild.iconURL({ extension: "png", size: 128 }) : null,
            lastSeen: row.last_seen || null,
        });
    }
    return out;
}

async function getGuildDashboardData(db, nowISO, getLatestPrice, client, guildId) {
    const [memberCountRes, purchaseCountRes, settingsRes, topMembersRes, recentPurchasesRes, latestPrice] = await Promise.all([
        db.query(`SELECT COUNT(*) AS c FROM members WHERE guild_id = $1`, [guildId]),
        db.query(`SELECT COUNT(*) AS c FROM purchases WHERE guild_id = $1`, [guildId]),
        db.query(`SELECT * FROM settings WHERE guild_id = $1`, [guildId]),
        db.query(
            `SELECT discord_id, balance_gold, updated_at
             FROM members
             WHERE guild_id = $1
             ORDER BY balance_gold DESC, updated_at DESC
             LIMIT 25`,
            [guildId]
        ),
        db.query(
            `SELECT discord_id, kind, details, gold_cost, balance_after, created_at
             FROM purchases
             WHERE guild_id = $1
             ORDER BY id DESC
             LIMIT 15`,
            [guildId]
        ),
        db.query(`SELECT * FROM prices WHERE guild_id = $1`, [guildId]).then(r => r.rows[0] || null).catch(async () => getLatestPrice()),
    ]);

    const allIds = [...topMembersRes.rows.map(r => r.discord_id), ...recentPurchasesRes.rows.map(r => r.discord_id)];
    const userLabels = await resolveUserLabels(client, allIds);
    const guild = client?.guilds?.cache?.get(guildId) || await client?.guilds?.fetch?.(guildId).catch(() => null);
    const guildIconUrl = guild?.iconURL ? guild.iconURL({ extension: "png", size: 128 }) : null;

    return {
        generatedAt: nowISO(),
        guildId,
        guildName: guild?.name || `Guild ${guildId}`,
        guildIconUrl,
        memberCount: toInt(memberCountRes.rows[0]?.c),
        purchaseCount: toInt(purchaseCountRes.rows[0]?.c),
        settingsCount: settingsRes.rows.length,
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

async function getDiscordOrderChannelUrl(db, preferredGuildId = null) {
    const guildId = preferredGuildId || process.env.GUILD_ID;
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
    const adminLink = [
        data.isAdmin
            ? `<a href="/admin" style="display:inline-block;padding:8px 12px;border-radius:10px;background:#1a2433;border:1px solid #2a3340;color:#e6edf3;text-decoration:none">Go to Admin Dashboard</a>`
            : "",
        `<a href="/guilds" style="display:inline-block;padding:8px 12px;border-radius:10px;background:#131d2b;border:1px solid #2a3340;color:#e6edf3;text-decoration:none;margin-left:${data.isAdmin ? "8px" : "0"}">Manage My Servers</a>`,
    ].filter(Boolean).join("");
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
    const orderHref = data.selectedGuildId ? `/me/order?guild=${encodeURIComponent(data.selectedGuildId)}` : "/me/order";
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
<div class="top"><div class="hero">${(data.guildIconUrl || data.avatarUrl) ? `<img src="${escapeHtml(data.guildIconUrl || data.avatarUrl)}" alt="server logo"/>` : ""}<div><h1 style="margin:0;font-size:18px">My Dashboard</h1><div style="margin-top:10px">${adminLink}</div></div></div><div><a href="/logout">Logout</a></div></div>
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
    <a href="${escapeHtml(orderHref)}" style="display:inline-block;padding:10px 14px;border-radius:10px;background:#1e2816;border:1px solid #304224;color:#e6edf3;text-decoration:none">Order Gold / Boost</a>
  </div>
</section>
<section class="panel"><h2 style="margin:0 0 10px">Your Recent Purchases</h2>
<table><thead><tr><th>Kind</th><th>Details</th><th>Cost</th><th>After</th><th>Time</th></tr></thead><tbody>${rows || '<tr><td colspan="5">No purchases yet.</td></tr>'}</tbody></table>
</section></div></body></html>`;
}

function renderGuildsHtml({ user, guilds }) {
    const rows = guilds.map((g) => {
        const roleLabel = g.canManage ? "Admin Dashboard" : "Member Dashboard";
        const roleBg = g.canManage ? "rgba(82, 196, 122, .12)" : "rgba(104, 163, 255, .12)";
        const roleBorder = g.canManage ? "rgba(82, 196, 122, .35)" : "rgba(104, 163, 255, .35)";
        const roleColor = g.canManage ? "#7be495" : "#8ab7ff";
        return `
      <a href="${escapeHtml(g.href || `/g/${encodeURIComponent(g.guildId)}`)}" style="display:flex;align-items:center;gap:14px;padding:14px;border:1px solid #2b3a4f;border-radius:14px;background:linear-gradient(180deg,#151d29,#121923);color:#e6edf3;text-decoration:none;box-shadow:inset 0 1px 0 rgba(255,255,255,.02);transition:transform .12s ease,border-color .12s ease">
        ${g.iconUrl ? `<img src="${escapeHtml(g.iconUrl)}" alt="" style="width:42px;height:42px;border-radius:50%;border:1px solid #314258;object-fit:cover"/>` : `<div style="width:42px;height:42px;border-radius:50%;background:radial-gradient(circle at 30% 30%,#243449,#182231);border:1px solid #314258"></div>`}
        <div style="flex:1;min-width:0">
          <div style="font-weight:700;font-size:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(g.name)}</div>
          <div style="margin-top:6px"><span style="display:inline-flex;align-items:center;padding:4px 9px;border-radius:999px;border:1px solid ${roleBorder};background:${roleBg};color:${roleColor};font-size:11px;font-weight:700;letter-spacing:.02em">${roleLabel}</span></div>
        </div>
        <div style="color:#9bc0ff;font-weight:700;font-size:13px">Open</div>
      </a>`;
    }).join("");
    const faviconUrl = getDashboardLogoUrl();
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Manage Servers</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>:root{--line:#243246;--text:#e6edf3;--muted:#9fb0c3}*{box-sizing:border-box}html,body{min-height:100%}body{margin:0;background-color:#080d13;background-image:radial-gradient(900px 380px at 8% -10%,rgba(104,163,255,.15),transparent 62%),radial-gradient(800px 320px at 100% 0%,rgba(46,204,113,.08),transparent 58%),linear-gradient(180deg,#07101a,#090f16 32%,#080d13 100%);background-repeat:no-repeat;background-attachment:fixed;color:var(--text);font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:980px;margin:0 auto;padding:24px 18px 34px}.top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}.title{margin:0;font-size:34px;line-height:1.05;font-weight:800;letter-spacing:-.02em}.muted{color:var(--muted)}.topbar-link{color:#b8d2ff;text-decoration:none;font-weight:600}.topbar-link:hover{text-decoration:underline}.panel{margin-top:16px;background:linear-gradient(180deg,rgba(18,26,38,.92),rgba(14,20,30,.96));border:1px solid var(--line);border-radius:18px;padding:16px;box-shadow:0 10px 28px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.02)}.panel-note{margin:0;padding:10px 12px;border-radius:12px;background:rgba(104,163,255,.08);border:1px solid rgba(104,163,255,.18);color:#bed3ef;font-size:13px}.grid{display:grid;gap:10px;margin-top:12px}.grid a:hover{border-color:#3b5472;transform:translateY(-1px)}.empty{padding:18px;border-radius:12px;border:1px dashed #2f4056;background:#121a25;color:var(--muted);text-align:center}@media (max-width:700px){.title{font-size:28px}.wrap{padding:18px 14px 28px}.top{align-items:stretch}.topbar-link{text-align:right}}</style></head>
<body><div class="wrap"><div class="top"><div><h1 class="title">Manage Servers</h1><div class="muted" style="margin-top:6px">${escapeHtml(user?.username || "Discord User")}</div></div><div><a class="topbar-link" href="/logout">Logout</a></div></div>
<section class="panel"><div class="grid" style="margin-top:0">${rows || '<div class="empty">No shared servers found for this Discord account.</div>'}</div></section></div></body></html>`;
}

function renderGuildAccessDeniedHtml() {
    const faviconUrl = getDashboardLogoUrl();
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Access Denied</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>body{margin:0;background:#0d1117;color:#e6edf3;font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:720px;margin:80px auto;padding:20px}.card{background:#161b22;border:1px solid #2a3340;border-radius:14px;padding:18px}a{color:#68a3ff;text-decoration:none}</style></head>
<body><div class="wrap"><div class="card"><h1 style="margin-top:0">Guild Access Denied</h1><p>You are logged in, but you do not have admin access to this guild dashboard.</p><p><a href="/guilds">Manage My Servers</a> · <a href="/me">My Dashboard</a></p></div></div></body></html>`;
}

function renderScopedDashboardHtml(data) {
    const html = renderDashboardHtml(data);
    return html
        .replace("Balance Bot Dashboard", `${escapeHtml(data.guildName || "Guild")} Dashboard`)
        .replace("Web view for balances, purchases, and price status.", `Guild dashboard for ${escapeHtml(data.guildName || "this server")}.`)
        .replace(
            /<div class="pill">([\s\S]*?)<\/div><\/div>/,
            `<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap"><a href="/guilds" style="color:#68a3ff;text-decoration:none">My Servers</a><a href="/me" style="color:#68a3ff;text-decoration:none">My Dashboard</a><a href="/logout" style="color:#68a3ff;text-decoration:none">Logout</a><div class="pill">$1</div></div></div>`
        )
        .replace('<div class="links"><a href="/api/overview"', `<div class="links"><a href="/guilds">Back to My Servers</a> · <a href="/api/g/${encodeURIComponent(data.guildId)}/overview"`);
}

function renderAdminScopedDashboardHtml(data) {
    const html = renderScopedDashboardHtml(data);
    return html.replace(
        '<a href="/me" style="color:#68a3ff;text-decoration:none">My Dashboard</a>',
        '<a href="/admin/global" style="color:#68a3ff;text-decoration:none">Global Overview</a><a href="/me" style="color:#68a3ff;text-decoration:none">My Dashboard</a>'
    );
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
    const [memberCountRes, purchaseCountRes, settingsCountRes, latestPrice, allPricesRes, topMembersRes, recentPurchasesRes] = await Promise.all([
        db.query(`SELECT COUNT(*) AS c FROM members`),
        db.query(`SELECT COUNT(*) AS c FROM purchases`),
        db.query(`SELECT COUNT(*) AS c FROM settings`),
        getLatestPrice(),
        db.query(`SELECT guild_id, usd_per_1m, updated_at FROM prices ORDER BY updated_at DESC`),
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
        pricesByGuild: allPricesRes.rows.map((r) => ({
            guild_id: String(r.guild_id),
            usd_per_1m: Number(r.usd_per_1m),
            updated_at: r.updated_at,
        })),
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
    const allPrices = data.pricesByGuild || [];
    const distinctPriceCount = new Set(allPrices.map((p) => String(p.usd_per_1m))).size;
    const hasMultipleGuildPrices = allPrices.length > 1;
    const showMixedPrices = hasMultipleGuildPrices && distinctPriceCount > 1;
    const mainGuildId = process.env.GUILD_ID ? String(process.env.GUILD_ID) : null;
    const mainGuildPrice = mainGuildId ? allPrices.find((p) => String(p.guild_id) === mainGuildId) : null;
    const primaryPrice = mainGuildPrice || data.latestPrice || null;
    const priceText = primaryPrice ? `${primaryPrice.usd_per_1m} USD / 1M` : "Not set";
    const priceSubtext = showMixedPrices
        ? `Main guild price · ${allPrices.length} guild price rows`
        : primaryPrice
            ? formatTimestamp(primaryPrice.updated_at)
            : "No price set";
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
<div class="card"><div class="k">${showMixedPrices ? "Main Guild Price" : "Latest Price"}</div><div class="v" style="font-size:18px">${escapeHtml(priceText)}</div><div class="sub">${escapeHtml(priceSubtext)}</div></div>
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
                const next = url.searchParams.get("next") || "/dashboard";
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
                    accessToken: token.access_token,
                    createdAt: Date.now(),
                });
                sendRedirect(
                    res,
                    oauthState?.next || "/dashboard",
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
                    sendRedirect(res, "/login?next=%2Fdashboard");
                    return;
                }
                const requestedGuildId = url.searchParams.get("guild") || null;
                const manageableGuilds = await getManageableGuilds(db, client, session.discordId);
                const userDataGuilds = await getUserDataGuilds(db, client, session.discordId);
                const validGuildIds = new Set([...manageableGuilds, ...userDataGuilds].map(g => g.guildId));
                if (session.accessToken) {
                    try {
                        const oauthGuilds = await fetchDiscordUserGuilds(session.accessToken);
                        for (const g of Array.isArray(oauthGuilds) ? oauthGuilds : []) {
                            if (g?.id) validGuildIds.add(String(g.id));
                        }
                    } catch {}
                }
                let selectedGuildId = requestedGuildId && validGuildIds.has(requestedGuildId) ? requestedGuildId : null;
                if (!selectedGuildId && userDataGuilds.length > 0) selectedGuildId = userDataGuilds[0].guildId;
                if (!selectedGuildId && manageableGuilds.length > 0) selectedGuildId = manageableGuilds[0].guildId;
                const data = await getUserDashboardData(db, client, session.discordId, selectedGuildId);
                data.manageableGuildCount = manageableGuilds.length;
                data.primaryManageGuildHref = manageableGuilds.length === 1 ? `/g/${encodeURIComponent(manageableGuilds[0].guildId)}` : null;
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderUserDashboardHtml(data));
                return;
            }

            if (url.pathname === "/dashboard" || url.pathname === "/guilds") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login?next=%2Fdashboard");
                    return;
                }
                let guilds = [];
                try {
                    const oauthGuilds = await fetchDiscordUserGuilds(session.accessToken);
                    guilds = (Array.isArray(oauthGuilds) ? oauthGuilds : []).map((g) => {
                        const guildId = String(g.id);
                        const botGuild = client?.guilds?.cache?.get(guildId);
                        if (!botGuild) return null;
                        const canManage = hasManageGuildPermissionBits(g.permissions, Boolean(g.owner));
                        return {
                            guildId,
                            name: botGuild.name || g.name || `Guild ${guildId}`,
                            iconUrl: botGuild.iconURL ? botGuild.iconURL({ extension: "png", size: 128 }) : (g.icon ? `https://cdn.discordapp.com/icons/${guildId}/${g.icon}.png?size=128` : null),
                            canManage,
                            href: canManage ? `/g/${encodeURIComponent(guildId)}` : `/me?guild=${encodeURIComponent(guildId)}`,
                        };
                    }).filter(Boolean).sort((a, b) => a.name.localeCompare(b.name));
                } catch {
                    const manageableGuilds = await getManageableGuilds(db, client, session.discordId);
                    guilds = manageableGuilds.map((g) => ({
                        ...g,
                        canManage: true,
                        href: `/g/${encodeURIComponent(g.guildId)}`,
                    }));
                }
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderGuildsHtml({ user: session, guilds }));
                return;
            }

            if (url.pathname === "/me/order") {
                if (!session?.discordId) {
                    sendRedirect(res, `/login?next=${encodeURIComponent(url.pathname)}`);
                    return;
                }
                const preferredGuildId = url.searchParams.get("guild") || null;
                const manageableGuilds = await getManageableGuilds(db, client, session.discordId);
                const userDataGuilds = await getUserDataGuilds(db, client, session.discordId);
                const validGuildIds = new Set([...manageableGuilds, ...userDataGuilds].map(g => g.guildId));
                const guildId = preferredGuildId && validGuildIds.has(preferredGuildId)
                    ? preferredGuildId
                    : (userDataGuilds[0]?.guildId || manageableGuilds[0]?.guildId || null);
                const orderUrl = await getDiscordOrderChannelUrl(db, guildId);
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
                const mainGuildId = process.env.GUILD_ID;
                if (!mainGuildId) {
                    const data = await getDashboardData(db, nowISO, getLatestPrice, client);
                    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                    res.end(renderDashboardHtml(data));
                    return;
                }
                const data = await getGuildDashboardData(db, nowISO, getLatestPrice, client, mainGuildId);
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderAdminScopedDashboardHtml(data));
                return;
            }

            if (url.pathname === "/admin/global") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login?next=%2Fadmin%2Fglobal");
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

            if (url.pathname.startsWith("/g/")) {
                if (!session?.discordId) {
                    sendRedirect(res, `/login?next=${encodeURIComponent(url.pathname)}`);
                    return;
                }
                const guildId = decodeURIComponent(url.pathname.slice(3)).trim();
                if (!guildId) {
                    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
                    res.end("Not found");
                    return;
                }
                const guilds = await getManageableGuilds(db, client, session.discordId);
                if (!guilds.some((g) => g.guildId === guildId) && !isAdminDiscordId(session.discordId)) {
                    res.writeHead(403, { "Content-Type": "text/html; charset=utf-8" });
                    res.end(renderGuildAccessDeniedHtml());
                    return;
                }
                const data = await getGuildDashboardData(db, nowISO, getLatestPrice, client, guildId);
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderScopedDashboardHtml(data));
                return;
            }

            if (url.pathname === "/") {
                if (!session?.discordId) {
                    sendRedirect(res, "/login?next=%2Fdashboard");
                    return;
                }
                sendRedirect(res, "/dashboard");
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

            if (url.pathname.startsWith("/api/g/") && url.pathname.endsWith("/overview")) {
                if (!session?.discordId) {
                    sendRedirect(res, "/login");
                    return;
                }
                const guildId = decodeURIComponent(url.pathname.slice("/api/g/".length, -"/overview".length)).trim();
                const guilds = await getManageableGuilds(db, client, session.discordId);
                if (!guilds.some((g) => g.guildId === guildId) && !isAdminDiscordId(session.discordId)) {
                    res.writeHead(403, { "Content-Type": "application/json; charset=utf-8" });
                    res.end(JSON.stringify({ error: "forbidden" }));
                    return;
                }
                const data = await getGuildDashboardData(db, nowISO, getLatestPrice, client, guildId);
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

