require("dotenv").config();

const {
    Client,
    GatewayIntentBits,
    Events,
    EmbedBuilder,
    ModalBuilder,
    TextInputBuilder,
    TextInputStyle,
    AttachmentBuilder,
    ActionRowBuilder,
    ButtonBuilder,
    ButtonStyle,
    ChannelType,
    PermissionsBitField,
} = require("discord.js");

const { Pool } = require("pg");
const { createCanvas, loadImage, registerFont } = require("canvas");

try {
    registerFont(require.resolve("dejavu-fonts-ttf/ttf/DejaVuSans.ttf"), {
        family: "JJDejaVu",
        weight: "normal",
    });
    registerFont(require.resolve("dejavu-fonts-ttf/ttf/DejaVuSans-Bold.ttf"), {
        family: "JJDejaVu",
        weight: "bold",
    });
} catch (err) {
    console.error("Font registration warning:", err?.message || err);
}

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
    throw new Error("DATABASE_URL is required. Add Railway Postgres DATABASE_URL to service variables.");
}

const db = new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes("localhost") || DATABASE_URL.includes("127.0.0.1")
        ? false
        : { rejectUnauthorized: false },
});

function toInt(value) {
    if (value === null || value === undefined) return 0;
    const n = Number.parseInt(String(value), 10);
    return Number.isFinite(n) ? n : 0;
}

async function initDatabase() {
    await db.query(`
CREATE TABLE IF NOT EXISTS settings (
  guild_id TEXT PRIMARY KEY,
  order_channel_id TEXT NOT NULL,
  gold_price_channel_id TEXT,
  tickets_category_id TEXT NOT NULL,
  archive_category_id TEXT
);
`);

    await db.query(`
CREATE TABLE IF NOT EXISTS prices (
  guild_id TEXT PRIMARY KEY,
  usd_per_1m DOUBLE PRECISION NOT NULL,
  updated_at TEXT NOT NULL
);
`);

    await db.query(`
CREATE TABLE IF NOT EXISTS members (
  discord_id TEXT PRIMARY KEY,
  balance_gold BIGINT NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);
`);

    await db.query(`
CREATE TABLE IF NOT EXISTS purchases (
  id BIGSERIAL PRIMARY KEY,
  discord_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  details TEXT NOT NULL,
  gold_cost BIGINT NOT NULL,
  balance_after BIGINT NOT NULL,
  created_at TEXT NOT NULL
);
`);

    await db.query(`
CREATE TABLE IF NOT EXISTS card_background_ownership (
  discord_id TEXT NOT NULL,
  bg_id TEXT NOT NULL,
  owned_at TEXT NOT NULL,
  PRIMARY KEY(discord_id, bg_id)
);
`);

    await db.query(`
CREATE TABLE IF NOT EXISTS card_profiles (
  discord_id TEXT PRIMARY KEY,
  equipped_bg_id TEXT NOT NULL DEFAULT 'common',
  updated_at TEXT NOT NULL
);
`);

    await db.query(`ALTER TABLE settings ADD COLUMN IF NOT EXISTS gold_price_channel_id TEXT`);
}

async function getSettings(guildId) {
    const res = await db.query(`SELECT * FROM settings WHERE guild_id = $1`, [guildId]);
    return res.rows[0] || null;
}

async function upsertSettings(guildId, orderChannelId, goldPriceChannelId, ticketsCategoryId, archiveCategoryId) {
    await db.query(
        `INSERT INTO settings(guild_id, order_channel_id, gold_price_channel_id, tickets_category_id, archive_category_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT(guild_id) DO UPDATE SET
           order_channel_id = EXCLUDED.order_channel_id,
           gold_price_channel_id = EXCLUDED.gold_price_channel_id,
           tickets_category_id = EXCLUDED.tickets_category_id,
           archive_category_id = EXCLUDED.archive_category_id`,
        [guildId, orderChannelId, goldPriceChannelId, ticketsCategoryId, archiveCategoryId]
    );
}

async function getPrice(guildId) {
    const res = await db.query(`SELECT usd_per_1m, updated_at FROM prices WHERE guild_id = $1`, [guildId]);
    return res.rows[0] || null;
}

async function getLatestPrice() {
    const res = await db.query(`SELECT guild_id, usd_per_1m, updated_at FROM prices ORDER BY updated_at DESC LIMIT 1`);
    return res.rows[0] || null;
}

async function upsertPrice(guildId, usdPer1m, updatedAt) {
    await db.query(
        `INSERT INTO prices(guild_id, usd_per_1m, updated_at)
         VALUES ($1, $2, $3)
         ON CONFLICT(guild_id) DO UPDATE SET
           usd_per_1m = EXCLUDED.usd_per_1m,
           updated_at = EXCLUDED.updated_at`,
        [guildId, usdPer1m, updatedAt]
    );
}

async function getMember(discordId) {
    const res = await db.query(`SELECT balance_gold, updated_at FROM members WHERE discord_id = $1`, [discordId]);
    if (!res.rows[0]) return null;
    return {
        balance_gold: toInt(res.rows[0].balance_gold),
        updated_at: res.rows[0].updated_at,
    };
}

async function insertMember(discordId, balanceGold, updatedAt) {
    await db.query(
        `INSERT INTO members(discord_id, balance_gold, updated_at) VALUES ($1, $2, $3)`,
        [discordId, balanceGold, updatedAt]
    );
}

async function updateMember(balanceGold, updatedAt, discordId) {
    await db.query(
        `UPDATE members SET balance_gold = $1, updated_at = $2 WHERE discord_id = $3`,
        [balanceGold, updatedAt, discordId]
    );
}

async function deleteMember(discordId) {
    const res = await db.query(`DELETE FROM members WHERE discord_id = $1`, [discordId]);
    return res.rowCount || 0;
}

async function insertPurchase(discordId, kind, details, goldCost, balanceAfter, createdAt) {
    await db.query(
        `INSERT INTO purchases(discord_id, kind, details, gold_cost, balance_after, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [discordId, kind, details, goldCost, balanceAfter, createdAt]
    );
}

async function getHistoryForUser(discordId, limit = 10) {
    const safeLimit = Math.max(1, Math.min(50, toInt(limit) || 10));
    const res = await db.query(
        `SELECT kind, details, gold_cost, balance_after, created_at
         FROM purchases
         WHERE discord_id = $1
         ORDER BY id DESC
         LIMIT $2`,
        [discordId, safeLimit]
    );
    return res.rows.map((r) => ({
        ...r,
        gold_cost: toInt(r.gold_cost),
        balance_after: toInt(r.balance_after),
    }));
}

async function deletePurchasesForUser(discordId) {
    const res = await db.query(`DELETE FROM purchases WHERE discord_id = $1`, [discordId]);
    return res.rowCount || 0;
}

async function getTotalBought(discordId) {
    const res = await db.query(
        `SELECT COALESCE(SUM(gold_cost), 0) AS total_gold FROM purchases WHERE discord_id = $1`,
        [discordId]
    );
    return { total_gold: toInt(res.rows[0]?.total_gold) };
}

async function getUserStats(discordId) {
    const [member, total, purchaseStats] = await Promise.all([
        getMember(discordId),
        getTotalBought(discordId),
        db.query(
            `SELECT COUNT(*) AS purchase_count, MAX(created_at) AS last_purchase_at
             FROM purchases
             WHERE discord_id = $1`,
            [discordId]
        ),
    ]);

    return {
        member,
        totalBoughtGold: total.total_gold,
        purchaseCount: toInt(purchaseStats.rows[0]?.purchase_count),
        lastPurchaseAt: purchaseStats.rows[0]?.last_purchase_at || null,
    };
}

async function countMembers() {
    const res = await db.query(`SELECT COUNT(*) AS c FROM members`);
    return toInt(res.rows[0]?.c);
}

async function countPurchases() {
    const res = await db.query(`SELECT COUNT(*) AS c FROM purchases`);
    return toInt(res.rows[0]?.c);
}

async function countSettings() {
    const res = await db.query(`SELECT COUNT(*) AS c FROM settings`);
    return toInt(res.rows[0]?.c);
}

const BUYER_TIERS = [
    { name: "Common", minGold: 0, color: 0x95a5a6 },
    { name: "Rare", minGold: 10_000_000, color: 0x3498db },
    { name: "Epic", minGold: 20_000_000, color: 0x9b59b6 },
    { name: "Legendary", minGold: 50_000_000, color: 0xf39c12 },
];
const CARD_BACKGROUNDS = [
    { id: "common", label: "Common", price: 0, palette: ["#2e3136", "#3b4048", "#24272d"], accent: "#95a5a6" },
    { id: "rare", label: "Rare", price: 5_000_000, palette: ["#0f2742", "#153f73", "#0a1830"], accent: "#3498db" },
    { id: "epic", label: "Epic", price: 12_000_000, palette: ["#2d1042", "#5a1f78", "#1e0a2c"], accent: "#9b59b6" },
    { id: "legendary", label: "Legendary", price: 30_000_000, palette: ["#4a2a00", "#8a4d00", "#2f1700"], accent: "#f39c12" },
];
const TIER_REWARDS = {
    Common: "No tier reward yet.",
    Rare: "20% off one key for every 4 keys bundle.",
    Epic: "1 free key on 8 keys bundle.",
    Legendary: "5% discount on every gold purchase + 1 free key on 8 keys bundle.",
};

function nowISO() {
    return new Date().toISOString();
}

function isManager(interaction) {
    return (
        interaction.member?.permissions?.has(PermissionsBitField.Flags.Administrator) ||
        interaction.member?.permissions?.has(PermissionsBitField.Flags.ManageGuild)
    );
}

function getAdminRoleOverwrites(guild) {
    const overwrites = [];
    for (const role of guild.roles.cache.values()) {
        if (role.id === guild.roles.everyone.id) continue;
        if (
            role.permissions.has(PermissionsBitField.Flags.Administrator) ||
            role.permissions.has(PermissionsBitField.Flags.ManageGuild)
        ) {
            overwrites.push({
                id: role.id,
                allow: [
                    PermissionsBitField.Flags.ViewChannel,
                    PermissionsBitField.Flags.SendMessages,
                    PermissionsBitField.Flags.ReadMessageHistory,
                ],
            });
        } else {
            overwrites.push({
                id: role.id,
                deny: [PermissionsBitField.Flags.ViewChannel],
            });
        }
    }
    return overwrites;
}

function formatGold(n) {
    if (n >= 1_000_000) {
        const v = n / 1_000_000;
        return `${v % 1 === 0 ? v.toFixed(0) : v.toFixed(2)}M`;
    }
    if (n >= 1_000) {
        const v = n / 1_000;
        return `${v % 1 === 0 ? v.toFixed(0) : v.toFixed(1)}k`;
    }
    return n.toString();
}

function isHttpUrl(value) {
    if (!value) return false;
    try {
        const u = new URL(value);
        return u.protocol === "http:" || u.protocol === "https:";
    } catch {
        return false;
    }
}

function parseEmbedColor(input) {
    if (!input) return null;
    const cleaned = String(input).trim().replace(/^#/, "");
    if (!/^[0-9a-fA-F]{6}$/.test(cleaned)) return null;
    return Number.parseInt(cleaned, 16);
}

function parseBooleanLike(input) {
    const v = String(input || "").trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(v);
}

function publishTextModal() {
    const modal = new ModalBuilder()
        .setCustomId("publishtext_modal")
        .setTitle("Publish Text");

    const content = new TextInputBuilder()
        .setCustomId("content")
        .setLabel("Message content")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setMaxLength(2000);

    const tts = new TextInputBuilder()
        .setCustomId("tts")
        .setLabel("TTS? (yes/no)")
        .setStyle(TextInputStyle.Short)
        .setRequired(false)
        .setPlaceholder("no");

    modal.addComponents(
        new ActionRowBuilder().addComponents(content),
        new ActionRowBuilder().addComponents(tts)
    );
    return modal;
}

function publishEmbedModal() {
    return publishEmbedModalWithToken("default");
}

function publishEmbedModalWithToken(token) {
    const modal = new ModalBuilder()
        .setCustomId(`publishembed_modal:${token}`)
        .setTitle("Publish Embed");

    const title = new TextInputBuilder()
        .setCustomId("title")
        .setLabel("Title")
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setMaxLength(256);

    const description = new TextInputBuilder()
        .setCustomId("description")
        .setLabel("Description")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setMaxLength(4000);

    const fieldsRaw = new TextInputBuilder()
        .setCustomId("fields_raw")
        .setLabel("Fields (one per line: Name | Value | inline)")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(false)
        .setPlaceholder("Price | 45 USD / 1M | yes");

    const messageText = new TextInputBuilder()
        .setCustomId("message_text")
        .setLabel("Text below fields (optional)")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(false)
        .setMaxLength(2000);

    modal.addComponents(
        new ActionRowBuilder().addComponents(title),
        new ActionRowBuilder().addComponents(description),
        new ActionRowBuilder().addComponents(fieldsRaw),
        new ActionRowBuilder().addComponents(messageText)
    );
    return modal;
}

const pendingEmbedModalDrafts = new Map();

function getTierForTotalBought(totalGold) {
    if (totalGold >= 50_000_000) return BUYER_TIERS[3];
    if (totalGold >= 20_000_000) return BUYER_TIERS[2];
    if (totalGold >= 10_000_000) return BUYER_TIERS[1];
    return BUYER_TIERS[0];
}

function getNextTier(totalGold) {
    for (const tier of BUYER_TIERS) {
        if (tier.minGold > totalGold) return tier;
    }
    return null;
}

function getNextTierProgress(totalGold) {
    const nextTier = getNextTier(totalGold);
    if (!nextTier) {
        return {
            nextTierLabel: "Legendary Tier (Max)",
            spendText: "Highest tier already unlocked.",
            nextReward: "5% discount on every gold purchase + 1 free key on 8 keys bundle.",
        };
    }

    const needed = nextTier.minGold - totalGold;
    return {
        nextTierLabel: `${nextTier.name} Tier`,
        spendText: `Spend ${formatGold(needed)} more to unlock ${nextTier.name} Tier.`,
        nextReward: TIER_REWARDS[nextTier.name] || "No reward configured.",
    };
}

function getTierByName(tierName) {
    return BUYER_TIERS.find((t) => t.name === tierName) || BUYER_TIERS[0];
}

function getTierNameFromMemberRoles(member) {
    if (!member) return "Common";
    if (member.roles.cache.some((r) => r.name === "Legendary")) return "Legendary";
    if (member.roles.cache.some((r) => r.name === "Epic")) return "Epic";
    if (member.roles.cache.some((r) => r.name === "Rare")) return "Rare";
    if (member.roles.cache.some((r) => r.name === "Common")) return "Common";
    return "Common";
}

function getBackgroundById(bgId) {
    return CARD_BACKGROUNDS.find((bg) => bg.id === bgId) || CARD_BACKGROUNDS[0];
}

function getBackgroundIdForTier(tierName) {
    const key = String(tierName || "Common").toLowerCase();
    if (key === "legendary") return "legendary";
    if (key === "epic") return "epic";
    if (key === "rare") return "rare";
    return "common";
}

async function resolveBrandLogoUrl(interaction) {
    if (interaction?.guild) {
        const inGuildIcon = interaction.guild.iconURL({ extension: "png", size: 512 });
        if (inGuildIcon) return inGuildIcon;
    }

    const fallbackGuildId = process.env.GUILD_ID;
    if (!fallbackGuildId) return null;

    const guild = await client.guilds.fetch(fallbackGuildId).catch(() => null);
    if (!guild) return null;
    return guild.iconURL({ extension: "png", size: 512 }) || null;
}

async function renderMemberCardImage(user, balanceGold, totalBoughtGold, tierName, bgId, brandLogoUrl) {
    const width = 1000;
    const height = 560;
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext("2d");
    const bg = getBackgroundById(bgId);

    const grad = ctx.createLinearGradient(0, 0, width, height);
    grad.addColorStop(0, bg.palette[0]);
    grad.addColorStop(0.6, bg.palette[1]);
    grad.addColorStop(1, bg.palette[2]);
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, width, height);

    ctx.globalAlpha = 0.14;
    ctx.fillStyle = bg.accent;
    ctx.beginPath();
    ctx.arc(width - 110, -30, 200, 0, Math.PI * 2);
    ctx.fill();
    ctx.beginPath();
    ctx.arc(width + 10, height + 10, 160, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalAlpha = 1;

    // subtle geometric texture
    ctx.fillStyle = "rgba(255,255,255,0.06)";
    for (let i = 0; i < 10; i += 1) {
        const x = 40 + i * 100;
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x + 120, 0);
        ctx.lineTo(x + 40, 120);
        ctx.closePath();
        ctx.fill();
    }

    ctx.fillStyle = "rgba(0,0,0,0.26)";
    ctx.fillRect(26, 26, width - 52, height - 52);
    ctx.strokeStyle = "rgba(255,255,255,0.45)";
    ctx.lineWidth = 2;
    ctx.strokeRect(26, 26, width - 52, height - 52);

    // two-part layout (no panel boxes)
    const cardX = 30;
    const cardY = 30;
    const cardW = width - 60;
    const cardH = height - 60;
    const gap = 36;
    const leftW = 430;
    const rightW = cardW - leftW - gap;

    const leftX = cardX + 18;
    const leftY = cardY + 18;
    const leftH = cardH - 36;

    const rightX = leftX + leftW + gap;
    const rightY = leftY;
    const rightH = leftH;

    ctx.fillStyle = "rgba(0,0,0,0.26)";
    ctx.fillRect(cardX, cardY, cardW, cardH);
    ctx.strokeStyle = "rgba(255,255,255,0.45)";
    ctx.lineWidth = 2;
    ctx.strokeRect(cardX, cardY, cardW, cardH);

    // left panel: logo + tier
    ctx.textAlign = "center";
    let hasBrandLogo = false;
    if (brandLogoUrl) {
        try {
            const logo = await loadImage(brandLogoUrl);
            const logoSize = 170;
            const logoX = leftX + Math.floor((leftW - logoSize) / 2);
            const logoY = leftY + 64;
            ctx.drawImage(logo, logoX, logoY, logoSize, logoSize);
            hasBrandLogo = true;
        } catch (err) {
            console.error("Failed to render brand logo:", err?.message || err);
        }
    }
    if (!hasBrandLogo) {
        const logoSize = 170;
        const logoX = leftX + Math.floor((leftW - logoSize) / 2);
        const logoY = leftY + 64;
        ctx.strokeStyle = bg.accent;
        ctx.lineWidth = 3;
        ctx.strokeRect(logoX, logoY, logoSize, logoSize);
    }
    ctx.fillStyle = bg.accent;
    ctx.font = "bold 42px 'JJDejaVu'";
    ctx.fillText(`${tierName} Tier`, leftX + leftW / 2, leftY + 300);
    ctx.fillStyle = "rgba(255,255,255,0.90)";
    ctx.font = "bold 38px 'JJDejaVu'";
    ctx.fillText(user.username, leftX + leftW / 2, leftY + 374);

    // right panel: avatar top, balances under
    const avatarSize = 180;
    const avatarX = rightX + Math.floor((rightW - avatarSize) / 2);
    const avatarY = rightY + 52;
    const avatarUrl = user.displayAvatarURL({ extension: "png", size: 256 });
    const avatar = await loadImage(avatarUrl);
    ctx.save();
    ctx.beginPath();
    ctx.arc(avatarX + avatarSize / 2, avatarY + avatarSize / 2, avatarSize / 2, 0, Math.PI * 2);
    ctx.closePath();
    ctx.clip();
    ctx.drawImage(avatar, avatarX, avatarY, avatarSize, avatarSize);
    ctx.restore();
    ctx.strokeStyle = bg.accent;
    ctx.lineWidth = 6;
    ctx.beginPath();
    ctx.arc(avatarX + avatarSize / 2, avatarY + avatarSize / 2, avatarSize / 2 + 2, 0, Math.PI * 2);
    ctx.stroke();

    ctx.textAlign = "center";
    const statsX = rightX + Math.floor(rightW / 2);
    const labelY1 = avatarY + avatarSize + 64;
    const valueY1 = labelY1 + 52;
    const labelY2 = valueY1 + 56;
    const valueY2 = labelY2 + 52;

    ctx.font = "bold 24px 'JJDejaVu'";
    ctx.fillStyle = "rgba(255,255,255,0.85)";
    ctx.fillText("Remaining Balance", statsX, labelY1);
    ctx.fillText("Total Spent Gold", statsX, labelY2);

    ctx.font = "bold 46px 'JJDejaVu'";
    ctx.fillStyle = "#f1f1f1";
    ctx.fillText(formatGold(balanceGold), statsX, valueY1);
    ctx.fillText(formatGold(totalBoughtGold), statsX, valueY2);

    return canvas.toBuffer("image/png");
}

function buildTierProgressEmbed(totalGold, tierName, userLabel) {
    const tier = getTierByName(tierName);
    const progress = getNextTierProgress(totalGold);
    const nextTier = getNextTier(totalGold);
    if (!nextTier) {
        return new EmbedBuilder()
            .setTitle("Tier Progress")
            .setColor(tier.color)
            .setDescription(
                `Thank you ${userLabel} for buying boost from us.\n` +
                `Total bought: ${formatGold(totalGold)} (${totalGold.toLocaleString()})\n` +
                `You have unlocked Legendary Tier.`
            )
            .addFields(
                { name: "Current Tier", value: `**${tierName} Tier**`, inline: true },
                { name: "Status", value: "Highest tier unlocked.", inline: true },
                { name: "Current Reward", value: TIER_REWARDS.Legendary, inline: false }
            );
    }

    return new EmbedBuilder()
        .setTitle("Tier Progress")
        .setColor(tier.color)
        .setDescription(
            `Thank you ${userLabel} for buying boost from us.\n` +
            `Total bought: ${formatGold(totalGold)} (${totalGold.toLocaleString()})\n` +
            `${progress.spendText}`
        )
        .addFields(
            { name: "Current Tier", value: `**${tierName} Tier**`, inline: true },
            { name: "Current Reward", value: TIER_REWARDS[tierName] || TIER_REWARDS.Common, inline: false },
            { name: "Next Tier", value: `**${progress.nextTierLabel}**`, inline: true },
            { name: "Next Reward", value: progress.nextReward, inline: false }
        );
}

function buildMemberCardEmbed(user, balanceGold, updatedAt, totalBoughtGold, tierName) {
    const tier = getTierByName(tierName);
    const avatarUrl = user.displayAvatarURL({ size: 256 });

    return new EmbedBuilder()
        .setColor(tier.color)
        .setTitle(`${tierName} Tier Member Card`)
        .setThumbnail(avatarUrl)
        .setDescription(`${user}`)
        .addFields(
            { name: "Discord Profile", value: `[Open Profile](https://discord.com/users/${user.id})`, inline: false },
            { name: "Role", value: `**${tierName} Tier**`, inline: true },
            { name: "Balance Remaining", value: `**${formatGold(balanceGold)}** (${balanceGold.toLocaleString()})`, inline: true },
            { name: "Total Bought", value: `**${formatGold(totalBoughtGold)}** (${totalBoughtGold.toLocaleString()})`, inline: false },
            { name: "Updated", value: `\`${updatedAt.replace("T", " ").slice(0, 19)}\``, inline: false }
        );
}

async function buildMemberCardMessage(user, balanceGold, updatedAt, totalBoughtGold, tierName, brandLogoUrl) {
    const bgId = getBackgroundIdForTier(tierName);
    const imageBuffer = await renderMemberCardImage(user, balanceGold, totalBoughtGold, tierName, bgId, brandLogoUrl);
    const fileName = `member-card-${user.id}.png`;
    const attachment = new AttachmentBuilder(imageBuffer, { name: fileName });
    return { files: [attachment] };
}

async function ensureTierRoles(guild) {
    const roleMap = new Map();

    for (const tier of BUYER_TIERS) {
        let role = guild.roles.cache.find((r) => r.name === tier.name);

        if (!role) {
            role = await guild.roles.create({
                name: tier.name,
                color: tier.color,
                reason: "Create buyer tier role",
            });
        } else if (role.color !== tier.color) {
            role = await role.edit({ color: tier.color, reason: "Sync buyer tier role color" });
        }

        roleMap.set(tier.name, role);
    }

    return roleMap;
}

async function syncBuyerTierRole(guild, userId, totalBoughtGold) {
    const member = await guild.members.fetch(userId);
    const roleMap = await ensureTierRoles(guild);
    const targetTier = getTierForTotalBought(totalBoughtGold);
    const targetRole = roleMap.get(targetTier.name);

    for (const tier of BUYER_TIERS) {
        const tierRole = roleMap.get(tier.name);
        if (!tierRole) continue;
        if (tier.name === targetTier.name) continue;
        if (member.roles.cache.has(tierRole.id)) {
            await member.roles.remove(tierRole, "Buyer tier changed");
        }
    }

    if (targetRole && !member.roles.cache.has(targetRole.id)) {
        await member.roles.add(targetRole, "Buyer tier sync");
    }

    return targetTier.name;
}

async function setBuyerTierRoleByName(guild, userId, tierName) {
    const member = await guild.members.fetch(userId);
    const roleMap = await ensureTierRoles(guild);
    const targetRole = roleMap.get(tierName);

    for (const tier of BUYER_TIERS) {
        const tierRole = roleMap.get(tier.name);
        if (!tierRole) continue;
        if (tier.name === tierName) continue;
        if (member.roles.cache.has(tierRole.id)) {
            await member.roles.remove(tierRole, "Buyer tier reset");
        }
    }

    if (targetRole && !member.roles.cache.has(targetRole.id)) {
        await member.roles.add(targetRole, "Buyer tier reset");
    }
}

async function clearBuyerTierRoles(guild, userId) {
    const member = await guild.members.fetch(userId);
    const roleMap = await ensureTierRoles(guild);

    for (const tier of BUYER_TIERS) {
        const tierRole = roleMap.get(tier.name);
        if (!tierRole) continue;
        if (member.roles.cache.has(tierRole.id)) {
            await member.roles.remove(tierRole, "Buyer full reset");
        }
    }
}

// ================= ORDER PANEL =================
function orderEmbed() {
    return new EmbedBuilder()
        .setTitle("Orders")
        .setDescription(
            [
                "Use the buttons below:",
                "- **Gold Price Check** = see latest gold rate",
                "- **Buy Gold** = open a gold ticket",
                "- **Buy Boost** = open a boost ticket",
            ].join("\n")
        );
}

function orderButtons() {
    return new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId("price_check")
            .setLabel("Gold Price Check")
            .setStyle(ButtonStyle.Secondary),

        new ButtonBuilder()
            .setCustomId("buy_gold")
            .setLabel("Buy Gold")
            .setStyle(ButtonStyle.Success),

        new ButtonBuilder()
            .setCustomId("buy_boost")
            .setLabel("Buy Boost")
            .setStyle(ButtonStyle.Primary)
    );
}

function isPriceStale(updatedAt) {
    if (!updatedAt) return true;
    const ts = Date.parse(updatedAt);
    if (!Number.isFinite(ts)) return true;
    return (Date.now() - ts) > 24 * 60 * 60 * 1000;
}

function goldPricePanelEmbed(currentPrice) {
    const stale = isPriceStale(currentPrice?.updated_at);
    const updatedAt = currentPrice?.updated_at
        ? `\`${String(currentPrice.updated_at).replace("T", " ").slice(0, 19)}\``
        : "`Not set`";
    const priceText = currentPrice ? `**${currentPrice.usd_per_1m} USD / 1M**` : "**Not set yet**";

    return new EmbedBuilder()
        .setTitle("Gold Price Check")
        .setColor(stale ? 0xf1c40f : 0x2ecc71)
        .setDescription(
            `Current price: ${priceText}\n` +
            `Last updated: ${updatedAt}\n\n` +
            `Note: Prices not updated within **1 day** are subject to change.\n` +
            `Please DM admin or click **Notify Admin** for the latest confirmed rate.`
        );
}

function goldPricePanelButtons() {
    return new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId("price_check")
            .setLabel("Check Current Price")
            .setStyle(ButtonStyle.Secondary),
        new ButtonBuilder()
            .setCustomId("notify_admin_price")
            .setLabel("Notify Admin")
            .setStyle(ButtonStyle.Primary)
    );
}

function resetButtons(token) {
    return new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`resetall_confirm:${token}`)
            .setLabel("Confirm Reset")
            .setStyle(ButtonStyle.Danger),
        new ButtonBuilder()
            .setCustomId(`resetall_cancel:${token}`)
            .setLabel("Cancel")
            .setStyle(ButtonStyle.Secondary)
    );
}

const pendingBalanceResets = new Map();
const TIP_BUTTON_ID = "tip_open";

function tipButtonRow() {
    return new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(TIP_BUTTON_ID)
            .setLabel("Tip Gold")
            .setStyle(ButtonStyle.Secondary)
    );
}

function tipModal() {
    const modal = new ModalBuilder()
        .setCustomId("tip_submit")
        .setTitle("Tip Gold");

    const amount = new TextInputBuilder()
        .setCustomId("amount")
        .setLabel("Tip amount (gold integer)")
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setPlaceholder("1000000");

    const note = new TextInputBuilder()
        .setCustomId("note")
        .setLabel("Note (optional)")
        .setStyle(TextInputStyle.Short)
        .setRequired(false)
        .setMaxLength(200);

    modal.addComponents(
        new ActionRowBuilder().addComponents(amount),
        new ActionRowBuilder().addComponents(note)
    );
    return modal;
}

async function createTicket(interaction, type) {
    const settings = await getSettings(interaction.guildId);
    if (!settings) {
        await interaction.editReply({ content: "ERROR: Bot not setup yet. Admin must run `/setup`." });
        return;
    }

    const guild = interaction.guild;
    const buyer = interaction.user;

    const safeName = buyer.username.toLowerCase().replace(/[^a-z0-9]/g, "");
    const suffix = buyer.id.slice(-4);
    const channelName = `ticket-${type}-${safeName || "buyer"}-${suffix}`.slice(0, 90);
    const adminRoleOverwrites = getAdminRoleOverwrites(guild);

    // Restrict to one active ticket per type per buyer in the active tickets category.
    const existingActive = guild.channels.cache.find((ch) => {
        if (ch.type !== ChannelType.GuildText) return false;
        if (ch.parentId !== settings.tickets_category_id) return false;
        if (!String(ch.name || "").startsWith(`ticket-${type}-`)) return false;
        const buyerOverwrite = ch.permissionOverwrites.cache.get(buyer.id);
        if (!buyerOverwrite) return false;
        return buyerOverwrite.allow.has(PermissionsBitField.Flags.ViewChannel);
    });
    if (existingActive) {
        await interaction.editReply({
            content: `WARNING: You already have an active ${type.toUpperCase()} ticket: <#${existingActive.id}>`,
        });
        return;
    }

    const ticketChannel = await guild.channels.create({
        name: channelName,
        type: ChannelType.GuildText,
        parent: settings.tickets_category_id,
        permissionOverwrites: [
            { id: guild.roles.everyone.id, deny: [PermissionsBitField.Flags.ViewChannel] },
            ...adminRoleOverwrites,
            {
                id: buyer.id,
                allow: [
                    PermissionsBitField.Flags.ViewChannel,
                    PermissionsBitField.Flags.SendMessages,
                    PermissionsBitField.Flags.ReadMessageHistory,
                ],
            },
            {
                id: interaction.client.user.id,
                allow: [
                    PermissionsBitField.Flags.ViewChannel,
                    PermissionsBitField.Flags.SendMessages,
                    PermissionsBitField.Flags.ManageChannels,
                    PermissionsBitField.Flags.ReadMessageHistory,
                ],
            },
        ],
    });

    if (type === "gold") {
        const p = await getPrice(interaction.guildId);
        const priceText = p ? `Current rate: **${p.usd_per_1m} USD / 1M**` : "Current rate: **Not set**";

        await ticketChannel.send({
            content:
                `[GOLD] **Ticket Type: GOLD**\n` +
                `Hi ${buyer}! Thanks for your gold order.\n` +
                `${priceText}\n\n` +
                `Please tell us:\n` +
                `1) How many gold (e.g., 1M, 2M)\n` +
                `2) Realm / region\n` +
                `3) Delivery method (mail / face-to-face, etc.)`,
        });

        await interaction.editReply({ content: `OK: Gold ticket created: <#${ticketChannel.id}>` });
        return;
    }

    await ticketChannel.send({
        content:
            `[BOOST] **Ticket Type: BOOST**\n` +
            `Hi ${buyer}! Let's set up your boost.\n\n` +
            `Please tell us:\n` +
            `1) Boost type (Mythic+ / Raid / etc.)\n` +
            `2) Details (e.g., 8 x +12)\n` +
            `3) Region/Realm + schedule/time\n` +
            `4) Any preferences (armor stack / traders / stream OFF, etc.)`,
    });

    await interaction.editReply({ content: `OK: Boost ticket created: <#${ticketChannel.id}>` });
}

// ================= BOT =================
const client = new Client({
    intents: [GatewayIntentBits.Guilds],
});

client.once(Events.ClientReady, () => {
    console.log(`OK: Logged in as ${client.user.tag}`);
});

client.on(Events.InteractionCreate, async (interaction) => {
    try {
        // =============== SLASH COMMANDS ===============
        if (interaction.isChatInputCommand()) {
            const { commandName } = interaction;

            if (commandName === "setup") {
                if (!isManager(interaction)) {
                    return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
                }

                await interaction.deferReply({ ephemeral: true });

                const existing = await getSettings(interaction.guildId);
                const orderOpt = interaction.options.getChannel("order_channel", false);
                const goldPriceOpt = interaction.options.getChannel("gold_price_channel", false);
                const ticketsOpt = interaction.options.getChannel("tickets_category", false);
                const archiveOpt = interaction.options.getChannel("archive_category", false);

                if (orderOpt && orderOpt.type !== ChannelType.GuildText) {
                    return interaction.editReply({ content: "ERROR: order_channel must be a text channel." });
                }
                if (goldPriceOpt && goldPriceOpt.type !== ChannelType.GuildText) {
                    return interaction.editReply({ content: "ERROR: gold_price_channel must be a text channel." });
                }
                if (ticketsOpt && ticketsOpt.type !== ChannelType.GuildCategory) {
                    return interaction.editReply({ content: "ERROR: tickets_category must be a category." });
                }
                if (archiveOpt && archiveOpt.type !== ChannelType.GuildCategory) {
                    return interaction.editReply({ content: "ERROR: archive_category must be a category." });
                }

                const orderChannelId = orderOpt?.id || existing?.order_channel_id || null;
                const goldPriceChannelId = goldPriceOpt?.id || existing?.gold_price_channel_id || null;
                const ticketsCategoryId = ticketsOpt?.id || existing?.tickets_category_id || null;
                const archiveCategoryId = archiveOpt?.id || existing?.archive_category_id || null;

                const missing = [];
                if (!orderChannelId) missing.push("order_channel");
                if (!goldPriceChannelId) missing.push("gold_price_channel");
                if (!ticketsCategoryId) missing.push("tickets_category");
                if (!archiveCategoryId) missing.push("archive_category");
                if (missing.length > 0) {
                    return interaction.editReply({
                        content:
                            `ERROR: Missing required setup value(s): ${missing.join(", ")}.\n` +
                            `Provide them in /setup now (only missing ones are required).`,
                    });
                }

                await upsertSettings(
                    interaction.guildId,
                    orderChannelId,
                    goldPriceChannelId,
                    ticketsCategoryId,
                    archiveCategoryId
                );

                const orderChannel = await interaction.guild.channels.fetch(orderChannelId).catch(() => null);
                const goldPriceChannel = await interaction.guild.channels.fetch(goldPriceChannelId).catch(() => null);
                const archiveCategory = await interaction.guild.channels.fetch(archiveCategoryId).catch(() => null);
                if (!orderChannel || orderChannel.type !== ChannelType.GuildText) {
                    return interaction.editReply({ content: "ERROR: Saved order channel is unavailable. Re-run /setup with order_channel." });
                }
                if (!goldPriceChannel || goldPriceChannel.type !== ChannelType.GuildText) {
                    return interaction.editReply({ content: "ERROR: Saved gold price channel is unavailable. Re-run /setup with gold_price_channel." });
                }

                await orderChannel.send({
                    embeds: [orderEmbed()],
                    components: [orderButtons()],
                });
                const currentPrice = await getPrice(interaction.guildId);
                await goldPriceChannel.send({
                    embeds: [goldPricePanelEmbed(currentPrice)],
                    components: [goldPricePanelButtons()],
                });

                return interaction.editReply({
                    content:
                        `OK: Setup saved.\n` +
                        `Order panel posted in <#${orderChannel.id}>.\n` +
                        `Gold price panel posted in <#${goldPriceChannel.id}>.\n` +
                        `Archive category: **${archiveCategory?.name || archiveCategoryId}**`,
                });
            }

            if (commandName === "goldprice") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                await interaction.deferReply({ ephemeral: true });

                const price = interaction.options.getNumber("usd_per_1m", true);
                if (price <= 0) return interaction.editReply({ content: "ERROR: Price must be > 0." });

                await upsertPrice(interaction.guildId, price, nowISO());
                const settings = await getSettings(interaction.guildId);
                if (settings?.gold_price_channel_id) {
                    const priceChannel = await interaction.guild.channels.fetch(settings.gold_price_channel_id).catch(() => null);
                    if (priceChannel && priceChannel.type === ChannelType.GuildText) {
                        await priceChannel.send({
                            embeds: [goldPricePanelEmbed({ usd_per_1m: price, updated_at: nowISO() })],
                            components: [goldPricePanelButtons()],
                        }).catch(() => null);
                    }
                }
                return interaction.editReply({ content: `OK: Price updated: ${price} USD / 1M` });
            }

            if (commandName === "dbcheck") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
                await interaction.deferReply({ ephemeral: true });

                const guildPrice = await getPrice(interaction.guildId);
                const latestPrice = await getLatestPrice();
                const m = await countMembers();
                const p = await countPurchases();
                const s = await countSettings();

                return interaction.editReply({
                    content:
                        `DB: Postgres (Railway)\n` +
                        `Guild: \`${interaction.guildId}\`\n` +
                        `Settings rows: **${s}**\n` +
                        `Members rows: **${m}**\n` +
                        `Purchases rows: **${p}**\n` +
                        `Current guild price: **${guildPrice ? guildPrice.usd_per_1m : "none"}**\n` +
                        `Latest price row: **${latestPrice ? `${latestPrice.usd_per_1m} (guild ${latestPrice.guild_id})` : "none"}**`,
                });
            }

            if (commandName === "postorder") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                await interaction.deferReply({ ephemeral: true });

                const settings = await getSettings(interaction.guildId);
                if (!settings) return interaction.editReply({ content: "ERROR: Run `/setup` first." });

                const orderCh = await client.channels.fetch(settings.order_channel_id).catch(() => null);
                if (!orderCh) return interaction.editReply({ content: "ERROR: Order channel not found. Re-run /setup." });
                const priceCh = settings.gold_price_channel_id
                    ? await client.channels.fetch(settings.gold_price_channel_id).catch(() => null)
                    : null;
                const currentPrice = await getPrice(interaction.guildId);

                await orderCh.send({ embeds: [orderEmbed()], components: [orderButtons()] });
                if (priceCh && priceCh.type === ChannelType.GuildText) {
                    await priceCh.send({ embeds: [goldPricePanelEmbed(currentPrice)], components: [goldPricePanelButtons()] });
                }
                return interaction.editReply({ content: "OK: Posted order panel and gold price panel." });
            }

            if (commandName === "deleteticket") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
                await interaction.deferReply({ ephemeral: true });

                const settings = await getSettings(interaction.guildId);
                if (!settings) return interaction.editReply({ content: "ERROR: Run `/setup` first." });
                if (!settings.archive_category_id) {
                    return interaction.editReply({ content: "ERROR: Archive category not configured. Re-run `/setup`." });
                }

                const channel = interaction.channel;
                if (!channel || channel.type !== ChannelType.GuildText) {
                    return interaction.editReply({ content: "ERROR: This command can only be used in a text ticket channel." });
                }

                if (channel.parentId === settings.archive_category_id) {
                    return interaction.editReply({ content: "WARNING: This ticket is already archived." });
                }

                const isTicketChannel =
                    channel.parentId === settings.tickets_category_id ||
                    String(channel.name || "").startsWith("ticket-");
                if (!isTicketChannel) {
                    return interaction.editReply({ content: "ERROR: This does not look like an active ticket channel." });
                }

                const archivedName = channel.name.startsWith("archived-")
                    ? channel.name
                    : `archived-${channel.name}`.slice(0, 100);

                await channel.setParent(settings.archive_category_id, { lockPermissions: true });
                if (channel.name !== archivedName) {
                    await channel.setName(archivedName);
                }

                return interaction.editReply({ content: "OK: Ticket archived to the configured archive category." });
            }

            if (commandName === "publishtext") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
                return interaction.showModal(publishTextModal());
            }

            if (commandName === "publishembed") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
                const channel = interaction.channel;
                if (!channel || channel.type !== ChannelType.GuildText) {
                    return interaction.reply({ content: "ERROR: This command must be used in a text channel.", ephemeral: true });
                }

                const picture = interaction.options.getAttachment("picture");
                if (picture && (!picture.contentType || !picture.contentType.startsWith("image/"))) {
                    return interaction.reply({ content: "ERROR: picture must be an image file.", ephemeral: true });
                }

                const token = `${interaction.user.id}:${Date.now()}`;
                pendingEmbedModalDrafts.set(token, {
                    authorId: interaction.user.id,
                    guildId: interaction.guildId,
                    channelId: channel.id,
                    imageUrl: picture?.url || "",
                    createdAt: Date.now(),
                });

                return interaction.showModal(publishEmbedModalWithToken(token));
            }

            if (commandName === "mc") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                await interaction.deferReply({ ephemeral: true });

                const user = interaction.options.getUser("user", true);
                const balance = interaction.options.getInteger("balance", true);
                if (balance < 0) return interaction.editReply({ content: "ERROR: Balance cannot be negative." });

                const existing = await getMember(user.id);
                if (existing) {
                    return interaction.editReply({
                        content: `WARNING: Member already exists with **${formatGold(existing.balance_gold)}**. Use **/addbal**.`,
                    });
                }

                const updatedAt = nowISO();
                await insertMember(user.id, balance, updatedAt);

                const totalBoughtGold = (await getTotalBought(user.id)).total_gold;
                let tierName = getTierForTotalBought(totalBoughtGold).name;
                try {
                    tierName = await syncBuyerTierRole(interaction.guild, user.id, totalBoughtGold);
                } catch (roleErr) {
                    console.error("Failed to sync tier role after /mc:", roleErr);
                }

                const guildLogoUrl = await resolveBrandLogoUrl(interaction);
                const cardMessage = await buildMemberCardMessage(user, balance, updatedAt, totalBoughtGold, tierName, guildLogoUrl);
                try {
                    const dmCardMessage = await buildMemberCardMessage(user, balance, updatedAt, totalBoughtGold, tierName, guildLogoUrl);
                    await user.send({
                        content: "Thank you for buying services from us. Here is your member card:",
                        files: dmCardMessage.files,
                    });
                } catch (dmErr) {
                    console.error("Failed to DM member card after /mc:", dmErr);
                }
                return interaction.editReply({
                    content: `OK: Member created for ${user}.`,
                    files: cardMessage.files,
                });
            }

            if (commandName === "addbal") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                await interaction.deferReply({ ephemeral: true });

                const user = interaction.options.getUser("user", true);
                const amount = interaction.options.getInteger("amount", true);
                if (amount <= 0) return interaction.editReply({ content: "ERROR: Amount must be > 0." });

                const existing = await getMember(user.id);
                if (!existing) return interaction.editReply({ content: `ERROR: No member record for ${user}. Use **/mc** first.` });

                const newBalance = existing.balance_gold + amount;
                await updateMember(newBalance, nowISO(), user.id);

                return interaction.editReply({
                    content: `OK: Added **${formatGold(amount)}** to ${user}. New balance: **${formatGold(newBalance)}**.`,
                });
            }

            if (commandName === "resetall") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                const user = interaction.options.getUser("user", true);
                const existing = await getMember(user.id);
                if (!existing) return interaction.reply({ content: `ERROR: No member record for ${user}. Use **/mc** first.`, ephemeral: true });

                const token = `${interaction.user.id}:${user.id}:${Date.now()}`;
                pendingBalanceResets.set(token, {
                    adminId: interaction.user.id,
                    targetUserId: user.id,
                    guildId: interaction.guildId,
                    oldBalance: existing.balance_gold,
                    createdAt: Date.now(),
                });

                return interaction.reply({
                    ephemeral: true,
                    content:
                        `Confirm full reset for ${user}?\n` +
                        `Current balance: **${formatGold(existing.balance_gold)}** (${existing.balance_gold.toLocaleString()})\n` +
                        `This will delete member card, purchase history, and tier roles.`,
                    components: [resetButtons(token)],
                });
            }

            if (commandName === "purchase") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                await interaction.deferReply({ ephemeral: true });

                const user = interaction.options.getUser("user", true);
                const kind = "boost";
                const details = interaction.options.getString("details", true);
                const goldCost = interaction.options.getInteger("gold_cost", true);
                if (goldCost <= 0) return interaction.editReply({ content: "ERROR: gold_cost must be > 0." });

                const member = await getMember(user.id);
                if (!member) return interaction.editReply({ content: `ERROR: ${user} has no member card. Use /mc first.` });

                const newBalance = member.balance_gold - goldCost;
                await updateMember(newBalance, nowISO(), user.id);
                await insertPurchase(user.id, kind, details, goldCost, newBalance, nowISO());
                const totalBoughtGold = (await getTotalBought(user.id)).total_gold;

                let tierName = getTierForTotalBought(totalBoughtGold).name;
                try {
                    tierName = await syncBuyerTierRole(interaction.guild, user.id, totalBoughtGold);
                } catch (roleErr) {
                    console.error("Failed to sync tier role:", roleErr);
                }

                try {
                    const progressEmbed = buildTierProgressEmbed(totalBoughtGold, tierName, `${user}`);
                    await user.send({ embeds: [progressEmbed], components: [tipButtonRow()] });
                } catch (dmErr) {
                    console.error("Failed to DM tier progress:", dmErr);
                }

                const embed = new EmbedBuilder()
                    .setTitle("OK: Purchase Recorded")
                    .setDescription(`${user}`)
                    .addFields((() => {
                        const progress = getNextTierProgress(totalBoughtGold);
                        return [
                            { name: "Type", value: kind.toUpperCase(), inline: true },
                            { name: "Details", value: details, inline: false },
                            { name: "Deducted", value: `-${formatGold(goldCost)} (${goldCost.toLocaleString()})`, inline: false },
                            { name: "Balance After", value: `**${formatGold(newBalance)}** (${newBalance.toLocaleString()})`, inline: false },
                            { name: "Total Bought", value: `**${formatGold(totalBoughtGold)}** (${totalBoughtGold.toLocaleString()})`, inline: false },
                            { name: "Tier", value: `**${tierName} Tier**`, inline: true },
                            { name: "Current Reward", value: TIER_REWARDS[tierName] || TIER_REWARDS.Common, inline: false },
                            { name: "Next Tier", value: `**${progress.nextTierLabel}**`, inline: true },
                            { name: "Next Reward", value: progress.nextReward, inline: false },
                            { name: "Progress", value: progress.spendText, inline: false },
                        ];
                    })());

                return interaction.editReply({ embeds: [embed], components: [tipButtonRow()] });
            }

            if (commandName === "history") {
                await interaction.deferReply({ ephemeral: true });

                const rows = await getHistoryForUser(interaction.user.id, 10);
                if (!rows || rows.length === 0) return interaction.editReply({ content: "No purchases yet." });

                const lines = rows.map((r, i) => {
                    const t = r.created_at.replace("T", " ").slice(0, 19);
                    return `**${i + 1}.** [${r.kind.toUpperCase()}] ${r.details} - -${formatGold(r.gold_cost)} | bal: ${formatGold(r.balance_after)}\n\`${t}\``;
                });

                return interaction.editReply({
                    embeds: [new EmbedBuilder().setTitle("Your Purchase History (Last 10)").setDescription(lines.join("\n\n"))],
                });
            }

            if (commandName === "historyuser") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });

                await interaction.deferReply({ ephemeral: true });

                const user = interaction.options.getUser("user", true);
                const rows = await getHistoryForUser(user.id, 10);
                if (!rows || rows.length === 0) return interaction.editReply({ content: `No purchases for ${user} yet.` });

                const lines = rows.map((r, i) => {
                    const t = r.created_at.replace("T", " ").slice(0, 19);
                    return `**${i + 1}.** [${r.kind.toUpperCase()}] ${r.details} - -${formatGold(r.gold_cost)} | bal: ${formatGold(r.balance_after)}\n\`${t}\``;
                });

                return interaction.editReply({
                    embeds: [new EmbedBuilder().setTitle(`Purchase History: ${user.username}`).setDescription(lines.join("\n\n"))],
                });
            }

            if (commandName === "who") {
                if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
                await interaction.deferReply({ ephemeral: true });

                const user = interaction.options.getUser("user", true);
                const stats = await getUserStats(user.id);
                if (!stats.member) {
                    return interaction.editReply({ content: `No member record for ${user}.` });
                }

                const tier = getTierForTotalBought(stats.totalBoughtGold);
                const progress = getNextTierProgress(stats.totalBoughtGold);
                const lastPurchase = stats.lastPurchaseAt
                    ? `\`${String(stats.lastPurchaseAt).replace("T", " ").slice(0, 19)}\``
                    : "None";

                const embed = new EmbedBuilder()
                    .setColor(tier.color)
                    .setTitle(`Member Stats: ${user.username}`)
                    .setDescription(`${user}`)
                    .addFields(
                        {
                            name: "Balance Remaining",
                            value: `**${formatGold(stats.member.balance_gold)}** (${stats.member.balance_gold.toLocaleString()})`,
                            inline: true,
                        },
                        {
                            name: "Total Spent Gold",
                            value: `**${formatGold(stats.totalBoughtGold)}** (${stats.totalBoughtGold.toLocaleString()})`,
                            inline: true,
                        },
                        {
                            name: "Tier",
                            value: `**${tier.name} Tier**`,
                            inline: true,
                        },
                        {
                            name: "Purchases",
                            value: `${stats.purchaseCount}`,
                            inline: true,
                        },
                        {
                            name: "Last Purchase",
                            value: lastPurchase,
                            inline: true,
                        },
                        {
                            name: "Next Tier Progress",
                            value: progress.spendText,
                            inline: false,
                        }
                    );

                return interaction.editReply({ embeds: [embed] });
            }

            if (commandName === "me") {
                await interaction.deferReply({ ephemeral: true });

                const row = await getMember(interaction.user.id);
                if (!row) return interaction.editReply({ content: "ERROR: You don't have a member record yet." });

                const totalBoughtGold = (await getTotalBought(interaction.user.id)).total_gold;
                let tierName;
                if (interaction.inGuild()) {
                    const guildMember = await interaction.guild.members.fetch(interaction.user.id).catch(() => null);
                    tierName = getTierNameFromMemberRoles(guildMember);
                } else {
                    tierName = getTierForTotalBought(totalBoughtGold).name;
                }
                const guildLogoUrl = await resolveBrandLogoUrl(interaction);
                const cardMessage = await buildMemberCardMessage(interaction.user, row.balance_gold, row.updated_at, totalBoughtGold, tierName, guildLogoUrl);
                return interaction.editReply({ files: cardMessage.files, components: [tipButtonRow()] });
            }

            return;
        }

        if (interaction.isModalSubmit()) {
            if (interaction.customId === "tip_submit") {
                const amountRaw = interaction.fields.getTextInputValue("amount").trim();
                const note = interaction.fields.getTextInputValue("note").trim();
                const amount = Number.parseInt(amountRaw, 10);
                if (!Number.isInteger(amount) || amount <= 0) {
                    return interaction.reply({ content: "ERROR: Tip amount must be a positive integer.", ephemeral: true });
                }

                const member = await getMember(interaction.user.id);
                if (!member) {
                    return interaction.reply({ content: "ERROR: You don't have a member record yet.", ephemeral: true });
                }
                if (member.balance_gold < amount) {
                    return interaction.reply({
                        content:
                            `ERROR: Not enough balance to tip.\n` +
                            `Current: **${formatGold(member.balance_gold)}** (${member.balance_gold.toLocaleString()})\n` +
                            `Need: **${formatGold(amount)}** (${amount.toLocaleString()})`,
                        ephemeral: true,
                    });
                }

                const newBalance = member.balance_gold - amount;
                await updateMember(newBalance, nowISO(), interaction.user.id);
                await insertPurchase(
                    interaction.user.id,
                    "tip",
                    note ? `Tip: ${note}` : "Tip",
                    amount,
                    newBalance,
                    nowISO()
                );

                const totalBoughtGold = (await getTotalBought(interaction.user.id)).total_gold;
                let tierName = getTierForTotalBought(totalBoughtGold).name;
                try {
                    const guild = interaction.inGuild()
                        ? interaction.guild
                        : await client.guilds.fetch(process.env.GUILD_ID).catch(() => null);
                    if (guild) {
                        tierName = await syncBuyerTierRole(guild, interaction.user.id, totalBoughtGold);
                    }
                } catch (err) {
                    console.error("Failed to sync tier after tip:", err);
                }

                const progress = getNextTierProgress(totalBoughtGold);
                return interaction.reply({
                    content:
                        `OK: Tip recorded: **-${formatGold(amount)}**\n` +
                        `Balance: **${formatGold(newBalance)}** (${newBalance.toLocaleString()})\n` +
                        `Tier: **${tierName} Tier**\n` +
                        `Next Tier: **${progress.nextTierLabel}**\n` +
                        `${progress.spendText}`,
                    ephemeral: true,
                });
            }

            if (!isManager(interaction)) return interaction.reply({ content: "ERROR: No permission.", ephemeral: true });
            const channel = interaction.channel;
            if (!channel || channel.type !== ChannelType.GuildText) {
                return interaction.reply({ content: "ERROR: This form must be submitted in a text channel.", ephemeral: true });
            }

            if (interaction.customId === "publishtext_modal") {
                const content = interaction.fields.getTextInputValue("content");
                const ttsRaw = interaction.fields.getTextInputValue("tts");
                const tts = parseBooleanLike(ttsRaw);
                const sent = await channel.send({ content, tts });
                return interaction.reply({ content: `OK: Text published.\n${sent.url}`, ephemeral: true });
            }

            if (interaction.customId.startsWith("publishembed_modal:")) {
                const token = interaction.customId.slice("publishembed_modal:".length);
                const draft = pendingEmbedModalDrafts.get(token);
                if (!draft) {
                    return interaction.reply({ content: "ERROR: Embed draft expired. Run /publishembed again.", ephemeral: true });
                }
                if (draft.authorId !== interaction.user.id) {
                    return interaction.reply({ content: "ERROR: Only the command user can submit this form.", ephemeral: true });
                }
                if (draft.guildId !== interaction.guildId) {
                    return interaction.reply({ content: "ERROR: This draft belongs to another server.", ephemeral: true });
                }
                if (Date.now() - draft.createdAt > 10 * 60 * 1000) {
                    pendingEmbedModalDrafts.delete(token);
                    return interaction.reply({ content: "ERROR: Draft timed out. Run /publishembed again.", ephemeral: true });
                }

                const title = interaction.fields.getTextInputValue("title").trim();
                const description = interaction.fields.getTextInputValue("description").trim();
                const fieldsRaw = interaction.fields.getTextInputValue("fields_raw").trim();
                const messageText = interaction.fields.getTextInputValue("message_text").trim();

                const fields = [];
                if (fieldsRaw) {
                    const lines = fieldsRaw.split("\n").map((l) => l.trim()).filter(Boolean);
                    if (lines.length > 10) {
                        return interaction.reply({ content: "ERROR: Maximum 10 field lines.", ephemeral: true });
                    }
                    for (const line of lines) {
                        const parts = line.split("|").map((p) => p.trim());
                        if (parts.length < 2 || !parts[0] || !parts[1]) {
                            return interaction.reply({
                                content: "ERROR: Field line format must be: Name | Value | inline(optional yes/no)",
                                ephemeral: true,
                            });
                        }
                        const inline = parts[2] ? parseBooleanLike(parts[2]) : false;
                        fields.push({ name: parts[0], value: parts[1], inline });
                    }
                }

                const targetChannel = interaction.guild.channels.cache.get(draft.channelId)
                    || await interaction.guild.channels.fetch(draft.channelId).catch(() => null);
                if (!targetChannel || targetChannel.type !== ChannelType.GuildText) {
                    pendingEmbedModalDrafts.delete(token);
                    return interaction.reply({ content: "ERROR: Target channel unavailable.", ephemeral: true });
                }

                const embed = new EmbedBuilder()
                    .setTitle(title)
                    .setDescription(description)
                    .setColor(0x3498db);
                if (fields.length > 0) embed.addFields(fields);
                if (messageText) {
                    embed.addFields({
                        name: "Additional Text",
                        value: messageText,
                        inline: false,
                    });
                }
                if (draft.imageUrl) embed.setImage(draft.imageUrl);
                const guildLogoUrl = interaction.guild?.iconURL({ extension: "png", size: 256 }) || null;
                if (guildLogoUrl) embed.setThumbnail(guildLogoUrl);

                const payload = { embeds: [embed] };

                const sent = await targetChannel.send(payload);
                pendingEmbedModalDrafts.delete(token);
                return interaction.reply({ content: `OK: Embed published.\n${sent.url}`, ephemeral: true });
            }
        }

        // =============== BUTTONS ===============
        if (interaction.isButton()) {
            if (interaction.customId === TIP_BUTTON_ID) {
                return interaction.showModal(tipModal());
            }

            if (interaction.customId.startsWith("resetall_confirm:") || interaction.customId.startsWith("resetall_cancel:")) {
                const splitIndex = interaction.customId.indexOf(":");
                const action = interaction.customId.slice(0, splitIndex);
                const token = interaction.customId.slice(splitIndex + 1);
                const payload = pendingBalanceResets.get(token);
                if (!payload) {
                    return interaction.reply({ content: "ERROR: This reset request is expired or invalid.", ephemeral: true });
                }

                if (payload.adminId !== interaction.user.id) {
                    return interaction.reply({ content: "ERROR: Only the admin who started this reset can confirm it.", ephemeral: true });
                }

                if (payload.guildId !== interaction.guildId) {
                    return interaction.reply({ content: "ERROR: This reset request belongs to another server.", ephemeral: true });
                }

                const maxAgeMs = 5 * 60 * 1000;
                if (Date.now() - payload.createdAt > maxAgeMs) {
                    pendingBalanceResets.delete(token);
                    return interaction.reply({ content: "ERROR: Reset confirmation timed out. Run /resetall again.", ephemeral: true });
                }

                if (action === "resetall_cancel") {
                    pendingBalanceResets.delete(token);
                    return interaction.update({ content: "OK: Reset canceled.", components: [] });
                }

                const member = await getMember(payload.targetUserId);
                if (!member) {
                    pendingBalanceResets.delete(token);
                    return interaction.update({ content: "ERROR: Member record no longer exists.", components: [] });
                }

                await deleteMember(payload.targetUserId);
                const deleted = await deletePurchasesForUser(payload.targetUserId);
                pendingBalanceResets.delete(token);
                try {
                    await clearBuyerTierRoles(interaction.guild, payload.targetUserId);
                } catch (roleErr) {
                    console.error("Failed to reset tier role:", roleErr);
                }

                return interaction.update({
                    content:
                        `OK: Member record deleted.\n` +
                        `OK: Total Spent Gold reset to **0**.\n` +
                        `Deleted purchase records: **${deleted}**.\n` +
                        `Previous balance: **${formatGold(member.balance_gold)}** (${member.balance_gold.toLocaleString()})\n` +
                        `Tier roles removed.`,
                    components: [],
                });
            }

            if (interaction.customId === "price_check") {
                const p = await getPrice(interaction.guildId);
                if (!p) return interaction.reply({ content: "WARNING: Price not set yet. Admin run `/goldprice`.", ephemeral: true });
                const stale = isPriceStale(p.updated_at);
                const staleLine = stale
                    ? "\nWARNING: This price is older than 1 day and may have changed."
                    : "\nOK: Price was updated within the last 24 hours.";

                return interaction.reply({
                    content:
                        `Gold Current Rate: **${p.usd_per_1m} USD / 1M**` +
                        `\nLast updated: \`${String(p.updated_at).replace("T", " ").slice(0, 19)}\`` +
                        staleLine,
                    ephemeral: true,
                });
            }

            if (interaction.customId === "notify_admin_price") {
                if (!interaction.inGuild()) {
                    return interaction.reply({ content: "ERROR: This button only works inside a server channel.", ephemeral: true });
                }
                const settings = await getSettings(interaction.guildId);
                if (!settings?.order_channel_id) {
                    return interaction.reply({ content: "ERROR: Setup not found. Ask admin to run `/setup`.", ephemeral: true });
                }
                const orderCh = await interaction.guild.channels.fetch(settings.order_channel_id).catch(() => null);
                if (!orderCh || orderCh.type !== ChannelType.GuildText) {
                    return interaction.reply({ content: "ERROR: Admin notify channel is unavailable.", ephemeral: true });
                }
                await orderCh.send(
                    `Price update request from ${interaction.user} in <#${interaction.channelId}>. ` +
                    `Please confirm the latest gold rate.`
                );
                return interaction.reply({ content: "OK: Admin has been notified for updated pricing.", ephemeral: true });
            }

            if (interaction.customId === "buy_gold") {
                await interaction.deferReply({ ephemeral: true });
                try {
                    await createTicket(interaction, "gold");
                } catch (e) {
                    console.error(e);
                    await interaction.editReply({ content: "ERROR: Failed to create GOLD ticket. Check bot permissions & category." });
                }
                return;
            }

            if (interaction.customId === "buy_boost") {
                await interaction.deferReply({ ephemeral: true });
                try {
                    await createTicket(interaction, "boost");
                } catch (e) {
                    console.error(e);
                    await interaction.editReply({ content: "ERROR: Failed to create BOOST ticket. Check bot permissions & category." });
                }
                return;
            }
        }
    } catch (err) {
        console.error(err);
        if (interaction.isRepliable()) {
            try {
                if (interaction.deferred || interaction.replied) {
                    await interaction.editReply({ content: "ERROR: Something went wrong." });
                } else {
                    await interaction.reply({ content: "ERROR: Something went wrong.", ephemeral: true });
                }
            } catch { }
        }
    }
});

async function start() {
    await initDatabase();
    await client.login(process.env.DISCORD_TOKEN);
}

start().catch((err) => {
    console.error("Fatal startup error:", err);
    process.exit(1);
});
