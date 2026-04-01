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

function formatRelativeTimestamp(ts) {
    if (!ts) return "-";
    const then = new Date(ts);
    if (Number.isNaN(then.getTime())) return "-";

    const diffMs = Date.now() - then.getTime();
    const absDiffMs = Math.abs(diffMs);
    const tense = diffMs >= 0 ? "ago" : "from now";
    const minuteMs = 60 * 1000;
    const hourMs = 60 * minuteMs;
    const dayMs = 24 * hourMs;

    if (absDiffMs < minuteMs) return "just now";
    if (absDiffMs < hourMs) {
        const minutes = Math.round(absDiffMs / minuteMs);
        return `${minutes} min${minutes === 1 ? "" : "s"} ${tense}`;
    }
    if (absDiffMs < dayMs) {
        const hours = Math.round(absDiffMs / hourMs);
        return `${hours} hour${hours === 1 ? "" : "s"} ${tense}`;
    }
    const days = Math.round(absDiffMs / dayMs);
    return `${days} day${days === 1 ? "" : "s"} ${tense}`;
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

function getTierForTotalBought(totalGold) {
    if (totalGold >= 50_000_000) return "Legendary";
    if (totalGold >= 20_000_000) return "Epic";
    if (totalGold >= 10_000_000) return "Rare";
    return "Common";
}

function getNextTierProgress(totalGold) {
    if (totalGold < 10_000_000) return `Spend ${formatGold(10_000_000 - totalGold)} more to unlock Rare Tier.`;
    if (totalGold < 20_000_000) return `Spend ${formatGold(20_000_000 - totalGold)} more to unlock Epic Tier.`;
    if (totalGold < 50_000_000) return `Spend ${formatGold(50_000_000 - totalGold)} more to unlock Legendary Tier.`;
    return "Highest tier already unlocked.";
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
    return fallback || process.env.DASHBOARD_LOGO_URL || null;
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

async function getGuildMemberDetailData(db, client, guildId, discordId) {
    const [memberRes, purchasesRes, totalSpentRes, totalsByKindRes, totalPurchaseOnlyRes] = await Promise.all([
        db.query(`SELECT balance_gold, updated_at FROM members WHERE guild_id = $1 AND discord_id = $2`, [guildId, discordId]),
        db.query(
            `SELECT kind, details, gold_cost, balance_after, created_at
             FROM purchases
             WHERE guild_id = $1 AND discord_id = $2
             ORDER BY created_at DESC, id DESC`,
            [guildId, discordId]
        ),
        db.query(
            `SELECT COALESCE(SUM(gold_cost), 0) AS total_gold
             FROM purchases
             WHERE guild_id = $1 AND discord_id = $2 AND kind <> 'withdraw'`,
            [guildId, discordId]
        ),
        db.query(
            `SELECT kind, COALESCE(SUM(gold_cost), 0) AS total_gold
             FROM purchases
             WHERE guild_id = $1 AND discord_id = $2
             GROUP BY kind`,
            [guildId, discordId]
        ),
        db.query(
            `SELECT COALESCE(SUM(gold_cost), 0) AS total_gold
             FROM purchases
             WHERE guild_id = $1 AND discord_id = $2
               AND kind NOT IN ('addbal', 'withdraw')`,
            [guildId, discordId]
        ),
    ]);

    const guild = client?.guilds?.cache?.get(guildId) || await client?.guilds?.fetch?.(guildId).catch(() => null);
    const user = client?.users?.cache?.get(discordId) || await client?.users?.fetch?.(discordId).catch(() => null);
    const guildMember = guild
        ? await guild.members.fetch({ user: discordId, force: true }).catch(() => null)
        : null;

    const totalSpentGold = toInt(totalSpentRes.rows[0]?.total_gold);
    const tierName = getTierForTotalBought(totalSpentGold);
    const totalsByKind = new Map(
        totalsByKindRes.rows.map((r) => [String(r.kind || "").toLowerCase(), toInt(r.total_gold)])
    );
    const totalAddedGold = totalsByKind.get("addbal") || 0;
    const totalWithdrawGold = totalsByKind.get("withdraw") || 0;
    const totalPurchaseGold = toInt(totalPurchaseOnlyRes.rows[0]?.total_gold);

    return {
        guildId,
        guildName: guild?.name || `Guild ${guildId}`,
        guildIconUrl: guild?.iconURL ? guild.iconURL({ extension: "png", size: 128 }) : null,
        discordId,
        username: user?.username || null,
        avatarUrl: user?.displayAvatarURL ? user.displayAvatarURL({ extension: "png", size: 128 }) : null,
        tierName,
        nextTierProgress: getNextTierProgress(totalSpentGold),
        totalSpentGold,
        totalAddedGold,
        totalWithdrawGold,
        totalPurchaseGold,
        member: memberRes.rows[0]
            ? {
                  balance_gold: toInt(memberRes.rows[0].balance_gold),
                  updated_at: memberRes.rows[0].updated_at,
              }
            : null,
        purchaseCount: purchasesRes.rows.length,
        purchases: purchasesRes.rows.map((r) => ({
            kind: r.kind,
            details: r.details,
            gold_cost: toInt(r.gold_cost),
            balance_after: toInt(r.balance_after),
            created_at: r.created_at,
        })),
        guildRoleNames: guildMember ? guildMember.roles.cache.map((r) => String(r.name || "")).filter(Boolean) : [],
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
    const tierName = data.guildRole || "Common";
    const tierPerkCopy = {
        Common: "No tier reward yet. Keep ordering to unlock Rare tier perks.",
        Rare: "Rare perk: 20% off one key for every 4 keys bundle.",
        Epic: "Epic perk: 1 free key on every 8 keys bundle.",
        Legendary: "Legendary perk: 5% discount on every gold purchase plus 1 free key on every 8 keys bundle.",
    };
    const tierPerkNote = tierPerkCopy[tierName] || tierPerkCopy.Common;
    const adminLink = data.isAdmin ? `<a class="ghost-link" href="/admin">Admin Dashboard</a>` : "";
    const tierColor = ({
        Legendary: "#f4c65d",
        Epic: "#d79cff",
        Rare: "#71d7ff",
        Common: "#b7c2cf",
    })[tierName] || "#b7c2cf";
    const tierTheme = ({
        Legendary: { glowA: "rgba(244,198,93,.20)", glowB: "rgba(222,122,46,.12)", surface: "#17120c" },
        Epic: { glowA: "rgba(215,156,255,.20)", glowB: "rgba(110,93,255,.12)", surface: "#13101b" },
        Rare: { glowA: "rgba(113,215,255,.18)", glowB: "rgba(50,191,165,.12)", surface: "#0f151b" },
        Common: { glowA: "rgba(183,194,207,.14)", glowB: "rgba(74,118,255,.10)", surface: "#10151b" },
    })[tierName] || {
        glowA: "rgba(183,194,207,.14)",
        glowB: "rgba(74,118,255,.10)",
        surface: "#10151b",
    };
    const faviconUrl = getDashboardLogoUrl(data.guildIconUrl || data.avatarUrl);
    const orderHref = data.selectedGuildId ? `/me/order?guild=${encodeURIComponent(data.selectedGuildId)}` : "/me/order";
    const featuredCards = [
        { title: "Mythic+ Runs", desc: "Fast key clears, armor stack options, and smooth repeat ordering.", accent: "#ffb347" },
        { title: "Raid Boosts", desc: "Heroic clears and scheduled runs presented like a real storefront.", accent: "#7dd3fc" },
        { title: "PvP Services", desc: "Rating help, cap pushes, and seasonal goals from one clean landing page.", accent: "#c084fc" },
    ];
    const featuredHtml = featuredCards.map((card) => `
      <article class="service-card">
        <div class="service-badge" style="--accent:${card.accent}">${escapeHtml(card.title)}</div>
        <p>${escapeHtml(card.desc)}</p>
        <a href="${escapeHtml(orderHref)}">Order Now</a>
      </article>`).join("");
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Storefront</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>
  :root{--bg:#090b10;--panel:#121722;--line:rgba(177,196,220,.14);--text:#eef4ff;--muted:#9aa9bf;--brand:#e8b15b;--brand-soft:rgba(232,177,91,.14);--link:#a7c8ff}
  *{box-sizing:border-box}
  body{margin:0;color:var(--text);font-family:Segoe UI,Arial,sans-serif;background:radial-gradient(720px 340px at 0% 0%, ${tierTheme.glowA}, transparent 60%),radial-gradient(720px 380px at 100% 0%, ${tierTheme.glowB}, transparent 60%),linear-gradient(180deg,#07090d 0%, #0a0e14 40%, #090b10 100%)}
  a{color:var(--link);text-decoration:none}a:hover{text-decoration:underline}
  .wrap{max-width:1140px;margin:0 auto;padding:24px 18px 40px}
  .topbar{display:flex;justify-content:space-between;align-items:center;gap:16px}
  .brand{display:flex;align-items:center;gap:12px}
  .brand img{width:44px;height:44px;border-radius:14px;object-fit:cover;border:1px solid var(--line)}
  .brand h1{margin:0;font-size:18px}
  .brand p{margin:4px 0 0;color:var(--muted);font-size:13px}
  .top-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
  .ghost-link,.top-actions a{display:inline-flex;align-items:center;justify-content:center;padding:10px 14px;border-radius:999px;border:1px solid var(--line);background:rgba(255,255,255,.02);color:var(--text);text-decoration:none}
  .hero{margin-top:18px;padding:28px;border-radius:28px;border:1px solid var(--line);background:radial-gradient(520px 260px at 100% 0%, rgba(232,177,91,.13), transparent 62%),radial-gradient(420px 240px at 0% 100%, rgba(111,213,255,.10), transparent 58%),linear-gradient(180deg, rgba(20,25,35,.96), rgba(13,17,24,.98));box-shadow:0 16px 50px rgba(0,0,0,.34), inset 0 1px 0 rgba(255,255,255,.03)}
  .hero-grid{display:grid;grid-template-columns:1.3fr .9fr;gap:18px;align-items:stretch}
  .eyebrow{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:var(--brand-soft);border:1px solid rgba(232,177,91,.24);color:#ffd08b;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase}
  .hero h2{margin:16px 0 10px;font-size:46px;line-height:.95;letter-spacing:-.04em;max-width:10ch}
  .hero-copy{max-width:58ch;color:#c6d1df;font-size:15px;line-height:1.65}
  .cta-row{display:flex;gap:12px;flex-wrap:wrap;margin-top:20px}
  .cta-primary,.cta-secondary{display:inline-flex;align-items:center;justify-content:center;padding:13px 18px;border-radius:14px;font-weight:700;text-decoration:none}
  .cta-primary{background:linear-gradient(135deg,#f0c46d,#c78639);color:#171108;border:1px solid rgba(255,215,151,.35)}
  .cta-secondary{background:rgba(255,255,255,.02);color:var(--text);border:1px solid var(--line)}
  .member-card{padding:20px;border-radius:22px;border:1px solid rgba(255,255,255,.08);background:radial-gradient(420px 240px at 100% -10%, ${tierTheme.glowA}, transparent 65%),linear-gradient(180deg, ${tierTheme.surface}, #0c1016)}
  .member-head{display:flex;gap:14px;align-items:center}
  .avatar-big{width:88px;height:88px;border-radius:22px;border:2px solid ${tierColor};object-fit:cover;background:#0b0f14}
  .tier-wrap{position:relative;display:inline-flex;align-items:flex-start}
  .tier-pill{display:inline-flex;padding:6px 10px;border-radius:999px;border:1px solid ${tierColor};color:${tierColor};background:rgba(255,255,255,.03);font-size:12px;font-weight:700;cursor:help}
  .tier-note{position:absolute;left:0;top:calc(100% + 10px);width:min(280px,70vw);padding:10px 12px;border-radius:14px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,#151b25,#0d1219);color:#d5deeb;font-size:12px;line-height:1.55;box-shadow:0 14px 30px rgba(0,0,0,.28);opacity:0;transform:translateY(4px);pointer-events:none;transition:opacity .14s ease,transform .14s ease;z-index:5}
  .tier-wrap:hover .tier-note,.tier-wrap:focus-within .tier-note{opacity:1;transform:translateY(0)}
  .member-name{margin:10px 0 4px;font-size:26px}
  .member-sub{color:var(--muted);font-size:13px}
  .metric-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:18px}
  .metric{padding:12px;border-radius:16px;background:#0d1118;border:1px solid rgba(255,255,255,.06)}
  .metric .k{color:var(--muted);font-size:12px}
  .metric .v{margin-top:6px;font-size:20px;font-weight:800}
  .mini-meta{margin-top:16px;color:#b7c4d6;font-size:13px}
  .section{margin-top:18px}
  .section-head{display:flex;justify-content:space-between;align-items:end;gap:12px;margin-bottom:12px}
  .section-head h3{margin:0;font-size:24px;letter-spacing:-.03em}
  .section-head p{margin:4px 0 0;color:var(--muted)}
  .service-grid,.trust-grid,.metric-grid{display:grid;gap:14px}
  .service-grid,.trust-grid{grid-template-columns:repeat(3,1fr)}
  .service-card{padding:18px;border-radius:20px;border:1px solid var(--line);background:linear-gradient(180deg,#121821,#0e131b)}
  .service-badge{display:inline-flex;padding:6px 10px;border-radius:999px;background:color-mix(in srgb, var(--accent) 15%, transparent);border:1px solid color-mix(in srgb, var(--accent) 32%, transparent);color:var(--accent);font-size:12px;font-weight:700}
  .service-card p{margin:14px 0 16px;color:#c6d1df;line-height:1.6;min-height:72px}
  .service-card a{display:inline-flex;padding:10px 14px;border-radius:12px;border:1px solid var(--line);background:#171e29;color:#eef4ff;text-decoration:none;font-weight:700}
  .panel{padding:20px;border-radius:22px;border:1px solid var(--line);background:linear-gradient(180deg,#111722,#0d121a)}
  .trust-card{padding:16px;border-radius:18px;border:1px solid rgba(255,255,255,.06);background:#0d1218}
  .trust-card strong{display:block;font-size:16px}
  .trust-card span{display:block;margin-top:6px;color:var(--muted);line-height:1.5;font-size:13px}
  table{width:100%;border-collapse:collapse}th,td{padding:10px 6px;border-bottom:1px solid rgba(255,255,255,.08);text-align:left;font-size:13px}th{color:var(--muted);font-weight:600}
  .empty-state{padding:18px;border-radius:16px;border:1px dashed rgba(255,255,255,.12);background:#0c1117;color:var(--muted)}
  @media(max-width:920px){.hero-grid,.service-grid,.trust-grid,.metric-grid{grid-template-columns:1fr}.hero h2{font-size:36px;max-width:none}}
  @media(max-width:640px){.wrap{padding:16px 14px 32px}.topbar{flex-direction:column;align-items:flex-start}.top-actions{width:100%}.top-actions a,.ghost-link{flex:1}.hero{padding:22px}.member-head{align-items:flex-start}}
</style></head><body><div class="wrap">
<header class="topbar">
  <div class="brand">
    ${(data.guildIconUrl || data.avatarUrl) ? `<img src="${escapeHtml(data.guildIconUrl || data.avatarUrl)}" alt="brand"/>` : ""}
    <div>
      <h1>Boost Store</h1>
      <p>Member-facing storefront with balance and ordering built in.</p>
    </div>
  </div>
  <div class="top-actions">
    ${adminLink}
    <a href="/guilds">Servers</a>
    <a href="/logout">Logout</a>
  </div>
</header>
<section class="hero">
  <div class="hero-grid">
    <div>
      <div class="eyebrow">Premium Game Services</div>
      <h2>Order like a storefront, not a control panel.</h2>
      <p class="hero-copy">This member page now feels closer to a sales landing page. Users still keep access to their balance, total spent, and purchase history, but the first thing they see is a cleaner storefront-style experience.</p>
      <div class="cta-row">
        <a class="cta-primary" href="${escapeHtml(orderHref)}">Start an Order</a>
        <a class="cta-secondary" href="#history">View Purchase History</a>
      </div>
    </div>
    <aside class="member-card">
      <div class="member-head">
        <div>${data.avatarUrl ? `<img class="avatar-big" src="${escapeHtml(data.avatarUrl)}" alt="avatar"/>` : `<div class="avatar-big"></div>`}</div>
        <div>
          <div class="tier-wrap">
            <div class="tier-pill" tabindex="0">${escapeHtml(tierName + " Tier")}</div>
            <div class="tier-note">${escapeHtml(tierPerkNote)}</div>
          </div>
          <div class="member-name">${escapeHtml(userLabel)}</div>
          <div class="member-sub">Private member overview for faster repeat orders.</div>
        </div>
      </div>
      <div class="metric-grid">
        <div class="metric"><div class="k">Balance</div><div class="v">${data.member ? escapeHtml(formatGold(data.member.balance_gold)) : "No Record"}</div></div>
        <div class="metric"><div class="k">Total Spent</div><div class="v">${escapeHtml(formatGold(data.totalSpentGold || 0))}</div></div>
        <div class="metric"><div class="k">Orders</div><div class="v">${data.purchases.length}</div></div>
      </div>
      <div class="mini-meta">Last updated: ${data.member ? escapeHtml(formatPrettyTimestamp(data.member.updated_at)) : "-"}</div>
    </aside>
  </div>
</section>
<section class="section">
  <div class="section-head">
    <div>
      <h3>Featured Services</h3>
      <p>Starter categories to make the page feel like a real storefront.</p>
    </div>
  </div>
  <div class="service-grid">${featuredHtml}</div>
</section>
<section class="section panel">
  <div class="section-head">
    <div>
      <h3>Why Members Use This</h3>
      <p>Trust-building copy sells better here than a plain dashboard heading.</p>
    </div>
  </div>
  <div class="trust-grid">
    <div class="trust-card"><strong>Fast repeat orders</strong><span>Members can jump into your order flow without hunting through Discord channels.</span></div>
    <div class="trust-card"><strong>Private account view</strong><span>Each user still sees only their own balance, spend, and history.</span></div>
    <div class="trust-card"><strong>Sales-first presentation</strong><span>The page now feels more like a boosting site and less like an internal admin tool.</span></div>
  </div>
</section>
<section id="history" class="section panel">
  <div class="section-head">
    <div>
      <h3>Recent Purchases</h3>
      <p>Your existing order history stays available below the storefront content.</p>
    </div>
  </div>
  ${rows ? `<table><thead><tr><th>Kind</th><th>Details</th><th>Cost</th><th>After</th><th>Time</th></tr></thead><tbody>${rows}</tbody></table>` : '<div class="empty-state">No purchases yet. Use the order button above to start your first order.</div>'}
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

function renderLandingPage({ loggedIn = false, logoUrl = null } = {}) {
    const faviconUrl = getDashboardLogoUrl(logoUrl);
    const primaryHref = "/login?next=%2Fme";
    const primaryLabel = "Login With Discord";
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>JJBoost</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>
  :root{--bg:#07090e;--line:rgba(177,196,220,.14);--text:#eef4ff;--gold:#e5b15d;--gold-deep:#b87934}
  *{box-sizing:border-box}
  body{margin:0;min-height:100vh;color:var(--text);font-family:Segoe UI,Arial,sans-serif;background:
    radial-gradient(720px 360px at 8% 0%, rgba(109,88,255,.18), transparent 62%),
    radial-gradient(640px 360px at 100% 10%, rgba(229,177,93,.16), transparent 60%),
    linear-gradient(180deg,#07090e 0%,#0a0f16 40%,#090c12 100%)}
  .wrap{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
  .shell{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:22px}
  .logo{width:88px;height:88px;border-radius:24px;border:1px solid var(--line);display:grid;place-items:center;overflow:hidden;background:linear-gradient(180deg,#171d28,#0e131b);color:var(--gold);font-weight:800;font-size:42px;box-shadow:0 16px 40px rgba(0,0,0,.32)}
  .logo img{width:100%;height:100%;object-fit:cover;display:block}
  .wordmark{font-size:34px;font-weight:800;letter-spacing:-.04em}
  .cta{display:inline-flex;align-items:center;justify-content:center;padding:14px 20px;border-radius:14px;text-decoration:none;font-weight:800}
  .cta-primary{background:linear-gradient(135deg,var(--gold),var(--gold-deep));color:#171108;border:1px solid rgba(255,219,165,.32)}
  .toplink{color:#dbe7f7;text-decoration:none;font-size:14px}
  @media(max-width:640px){.logo{width:76px;height:76px;font-size:36px}.wordmark{font-size:28px}}
</style></head><body><div class="wrap">
<div class="shell">
  <div class="logo">${logoUrl ? `<img src="${escapeHtml(logoUrl)}" alt="JJBoost logo"/>` : "J"}</div>
  <div class="wordmark">JJBoost</div>
  <a class="cta cta-primary" href="${escapeHtml(primaryHref)}">${escapeHtml(primaryLabel)}</a>
</div></div></body></html>`;
}

function renderGuildMemberDetailHtml(data) {
    const userLabel = data.username || `User ${shortDiscordId(data.discordId)}`;
    const rolePreview = data.guildRoleNames.length ? escapeHtml(data.guildRoleNames.slice(0, 8).join(", ")) : "No role data";
    const sortedActivity = [...data.purchases].sort((a, b) => {
        const aTime = new Date(a?.created_at || 0).getTime();
        const bTime = new Date(b?.created_at || 0).getTime();
        return bTime - aTime;
    });
    const activityKinds = [...new Set(sortedActivity.map((p) => String(p.kind || "").toUpperCase()).filter(Boolean))];
    const renderRows = (items) => items.map((p) => `
      <tr data-kind="${escapeHtml(String(p.kind || "").toUpperCase())}">
        <td>${escapeHtml(String(p.kind || "").toUpperCase())}</td>
        <td title="${escapeHtml(p.details)}">${escapeHtml(String(p.details || "").slice(0, 60))}</td>
        <td>${String(p.kind || "").toLowerCase() === "addbal" ? "+" : "-"}${escapeHtml(formatGold(p.gold_cost))}</td>
        <td>${escapeHtml(formatGold(p.balance_after))}</td>
        <td><div style="display:flex;justify-content:space-between;gap:12px;align-items:center"><small>${formatTimestamp(p.created_at)}</small><small style="color:#9fb0c3;white-space:nowrap">${escapeHtml(formatRelativeTimestamp(p.created_at))}</small></div></td>
      </tr>`).join("");
    const tierColor = ({
        Legendary: "#f39c12",
        Epic: "#9b59b6",
        Rare: "#3498db",
        Common: "#95a5a6",
    })[data.tierName] || "#95a5a6";
    const faviconUrl = getDashboardLogoUrl(data.guildIconUrl || data.avatarUrl);
    return `<!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Member Detail</title>${faviconUrl ? `<link rel="icon" href="${escapeHtml(faviconUrl)}"/>` : ""}
<style>
body{margin:0;background:#0d1117;color:#e6edf3;font-family:Segoe UI,Arial,sans-serif}
.wrap{max-width:1040px;margin:0 auto;padding:20px}
.top{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}
.links a{color:#68a3ff;text-decoration:none;margin-right:10px}
.panel{margin-top:14px;background:#161b22;border:1px solid #2a3340;border-radius:14px;padding:14px}
.hero{display:grid;grid-template-columns:110px 1fr;gap:14px;align-items:center}
.avatar{width:96px;height:96px;border-radius:50%;border:3px solid ${tierColor};object-fit:cover;background:#10151d}
.tier{display:inline-block;padding:6px 10px;border-radius:999px;border:1px solid ${tierColor};color:${tierColor};font-size:12px;font-weight:700}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:12px}
.card{background:#10151d;border:1px solid #2a3340;border-radius:12px;padding:10px}
.k{color:#9fb0c3;font-size:12px}.v{font-size:20px;font-weight:700;margin-top:4px}
table{width:100%;border-collapse:collapse}th,td{padding:8px 6px;border-bottom:1px solid #2a3340;text-align:left;font-size:13px}th{color:#9fb0c3}
.history-toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px}
.history-toolbar select{background:#10151d;border:1px solid #2a3340;border-radius:10px;color:#e6edf3;padding:8px 10px}
.history-empty{display:none;padding:14px 8px;color:#9fb0c3;text-align:center}
@media (max-width:800px){.stats{grid-template-columns:1fr 1fr}.hero{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<div class="top">
<h1 style="margin:0;font-size:20px">Member Detail</h1>
<div class="links"><a href="/g/${encodeURIComponent(data.guildId)}">Back to Guild Dashboard</a><a href="/guilds">My Servers</a><a href="/logout">Logout</a></div>
</div>
<section class="panel">
<div class="hero">
<div>${data.avatarUrl ? `<img class="avatar" src="${escapeHtml(data.avatarUrl)}" alt="avatar"/>` : `<div class="avatar"></div>`}</div>
<div>
<div class="tier">${escapeHtml(data.tierName)} Tier</div>
<h2 style="margin:8px 0 4px">${escapeHtml(userLabel)}</h2>
<div style="color:#9fb0c3"><code>${escapeHtml(data.discordId)}</code></div>
<div style="color:#9fb0c3;margin-top:6px">Roles: ${rolePreview}</div>
</div>
</div>
<div class="stats">
<div class="card"><div class="k">Balance</div><div class="v">${data.member ? escapeHtml(formatGold(data.member.balance_gold)) : "No Record"}</div></div>
<div class="card"><div class="k">Total Spent (Tier)</div><div class="v">${escapeHtml(formatGold(data.totalSpentGold || 0))}</div></div>
<div class="card"><div class="k">Total Added Gold</div><div class="v">${escapeHtml(formatGold(data.totalAddedGold || 0))}</div></div>
<div class="card"><div class="k">Total Withdraw Gold</div><div class="v">${escapeHtml(formatGold(data.totalWithdrawGold || 0))}</div></div>
<div class="card"><div class="k">Total Purchase Gold</div><div class="v">${escapeHtml(formatGold(data.totalPurchaseGold || 0))}</div></div>
<div class="card"><div class="k">Activity Rows (Shown)</div><div class="v">${data.purchaseCount}</div></div>
<div class="card"><div class="k">Last Updated</div><div class="v" style="font-size:14px">${data.member ? formatPrettyTimestamp(data.member.updated_at) : "-"}</div></div>
</div>
<div style="margin-top:10px;color:#9fb0c3">${escapeHtml(data.nextTierProgress)}</div>
</section>
<section class="panel">
<div class="history-toolbar">
<h2 style="margin:0">Activity History</h2>
<label style="display:flex;align-items:center;gap:8px;color:#9fb0c3;font-size:13px">Kind
<select id="activity-kind-filter">
<option value="ALL">All kinds</option>
${activityKinds.map((kind) => `<option value="${escapeHtml(kind)}">${escapeHtml(kind)}</option>`).join("")}
</select>
</label>
</div>
<table><thead><tr><th>Kind</th><th>Details</th><th>Amount</th><th>Balance After</th><th>Time</th></tr></thead><tbody id="activity-history-body">${renderRows(sortedActivity) || '<tr><td colspan="5">No activity history.</td></tr>'}</tbody></table>
<div id="activity-history-empty" class="history-empty">No activity for this kind.</div>
</section>
<script>
(() => {
  const filter = document.getElementById("activity-kind-filter");
  const body = document.getElementById("activity-history-body");
  const empty = document.getElementById("activity-history-empty");
  if (!filter || !body) return;
  const rows = Array.from(body.querySelectorAll("tr[data-kind]"));
  const applyFilter = () => {
    const selected = filter.value;
    let visible = 0;
    for (const row of rows) {
      const show = selected === "ALL" || row.getAttribute("data-kind") === selected;
      row.style.display = show ? "" : "none";
      if (show) visible += 1;
    }
    if (empty) empty.style.display = visible === 0 ? "block" : "none";
  };
  filter.addEventListener("change", applyFilter);
  applyFilter();
})();
</script>
</div></body></html>`;
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
    const detailHref = (discordId) => (
        data.guildId
            ? `/g/${encodeURIComponent(data.guildId)}/member/${encodeURIComponent(String(discordId || ""))}`
            : null
    );
    const topRows = data.topMembers.map((m, i) => {
        const href = detailHref(m.discord_id);
        const userLabel = escapeHtml(m.user_label || "Unknown");
        const labelHtml = href ? `<a href="${escapeHtml(href)}" style="color:#8ab7ff;text-decoration:none">${userLabel}</a>` : userLabel;
        return `<tr><td>${i + 1}</td><td>${labelHtml}<br/><small><code>${escapeHtml(shortDiscordId(m.discord_id))}</code></small></td><td>${escapeHtml(formatGold(m.balance_gold))}</td><td><small>${formatTimestamp(m.updated_at)}</small></td></tr>`;
    }).join("");
    const purchaseRows = data.recentPurchases.map((p) => {
        const href = detailHref(p.discord_id);
        const userLabel = escapeHtml(p.user_label || "Unknown");
        const labelHtml = href ? `<a href="${escapeHtml(href)}" style="color:#8ab7ff;text-decoration:none">${userLabel}</a>` : userLabel;
        return `<tr><td>${labelHtml}<br/><small><code>${escapeHtml(shortDiscordId(p.discord_id))}</code></small></td><td>${escapeHtml(String(p.kind || "").toUpperCase())}</td><td title="${escapeHtml(p.details)}">${escapeHtml(String(p.details || "").slice(0, 32))}</td><td>-${escapeHtml(formatGold(p.gold_cost))}</td><td>${escapeHtml(formatGold(p.balance_after))}</td><td><small>${formatTimestamp(p.created_at)}</small></td></tr>`;
    }).join("");
    const lookupForm = data.guildId
        ? `<section class="panel" style="margin-bottom:12px">
<h2>Member Lookup</h2>
<form method="GET" action="/g/${encodeURIComponent(data.guildId)}/member" style="display:flex;gap:8px;flex-wrap:wrap">
<input name="user" placeholder="Discord User ID" required style="background:#0f141b;border:1px solid #2a3340;border-radius:10px;padding:10px 12px;color:#e6edf3;min-width:280px"/>
<button type="submit" style="background:#2a4d8f;color:#fff;border:1px solid #3c66b2;border-radius:10px;padding:10px 14px;cursor:pointer">Open Member</button>
</form>
</section>`
        : "";

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
${lookupForm}
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

            if (url.pathname === "/") {
                if (session?.discordId) {
                    sendRedirect(res, "/me");
                    return;
                }
                const botLogoUrl = client?.user?.displayAvatarURL
                    ? client.user.displayAvatarURL({ extension: "png", size: 256 })
                    : null;
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderLandingPage({ loggedIn: false, logoUrl: botLogoUrl }));
                return;
            }

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
                    accessToken: token.access_token,
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
                    sendRedirect(res, "/");
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
                sendRedirect(res, session?.discordId ? "/me" : "/");
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

            if (url.pathname.startsWith("/g/") && url.pathname.endsWith("/member")) {
                if (!session?.discordId) {
                    sendRedirect(res, `/login?next=${encodeURIComponent(url.pathname + (url.search || ""))}`);
                    return;
                }
                const rest = url.pathname.slice(3, -"/member".length);
                const guildId = decodeURIComponent(rest).trim().replace(/\/$/, "");
                const targetUserId = (url.searchParams.get("user") || "").trim();
                if (!guildId || !targetUserId) {
                    res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
                    res.end("Missing guild or user id");
                    return;
                }
                const guilds = await getManageableGuilds(db, client, session.discordId);
                if (!guilds.some((g) => g.guildId === guildId) && !isAdminDiscordId(session.discordId)) {
                    res.writeHead(403, { "Content-Type": "text/html; charset=utf-8" });
                    res.end(renderGuildAccessDeniedHtml());
                    return;
                }
                sendRedirect(res, `/g/${encodeURIComponent(guildId)}/member/${encodeURIComponent(targetUserId)}`);
                return;
            }

            if (url.pathname.startsWith("/g/") && url.pathname.includes("/member/")) {
                if (!session?.discordId) {
                    sendRedirect(res, `/login?next=${encodeURIComponent(url.pathname)}`);
                    return;
                }
                const memberMarker = "/member/";
                const markerIdx = url.pathname.indexOf(memberMarker);
                const guildId = decodeURIComponent(url.pathname.slice(3, markerIdx)).trim();
                const targetUserId = decodeURIComponent(url.pathname.slice(markerIdx + memberMarker.length)).trim();
                if (!guildId || !targetUserId) {
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
                const data = await getGuildMemberDetailData(db, client, guildId, targetUserId);
                res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                res.end(renderGuildMemberDetailHtml(data));
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

