require("dotenv").config();
const { REST, Routes, SlashCommandBuilder, PermissionFlagsBits, ChannelType } = require("discord.js");

const commands = [
    new SlashCommandBuilder()
        .setName("setup")
        .setDescription("Save order channel + tickets category + archive category for this server")
        .addChannelOption(o =>
            o.setName("order_channel")
                .setDescription("Select the #order channel")
                .setRequired(true)
                .addChannelTypes(ChannelType.GuildText)
        )
        .addChannelOption(o =>
            o.setName("tickets_category")
                .setDescription("Select the Tickets category")
                .setRequired(true)
                .addChannelTypes(ChannelType.GuildCategory)
        )
        .addChannelOption(o =>
            o.setName("archive_category")
                .setDescription("Select the Archive category")
                .setRequired(true)
                .addChannelTypes(ChannelType.GuildCategory)
        )
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("goldprice")
        .setDescription("Update gold price (USD per 1M)")
        .addNumberOption(o =>
            o.setName("usd_per_1m")
                .setDescription("Example: 45")
                .setRequired(true)
        )
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("dbcheck")
        .setDescription("Show runtime DB status (admin)")
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("postorder")
        .setDescription("Post the order embed + buttons in the saved order channel")
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("deleteticket")
        .setDescription("Archive this ticket channel into the configured archive category")
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("publishtext")
        .setDescription("Open a form to publish plain text in this channel")
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("publishembed")
        .setDescription("Open embed form; optional picture can be attached first")
        .addAttachmentOption(o => o.setName("picture").setDescription("Optional image upload").setRequired(false))
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("mc")
        .setDescription("MemberCreate: create a member card (one-time)")
        .addUserOption(o => o.setName("user").setDescription("Buyer").setRequired(true))
        .addIntegerOption(o => o.setName("balance").setDescription("Gold balance (1000000 = 1M)").setRequired(true))
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("addbal")
        .setDescription("Add gold to a buyer's balance")
        .addUserOption(o => o.setName("user").setDescription("Buyer").setRequired(true))
        .addIntegerOption(o => o.setName("amount").setDescription("Gold amount to add (1000000 = 1M)").setRequired(true))
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("resetall")
        .setDescription("Reset a buyer's balance + total bought to 0 (requires confirmation)")
        .addUserOption(o => o.setName("user").setDescription("Buyer").setRequired(true))
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("me")
        .setDescription("View your member card"),

    new SlashCommandBuilder()
        .setName("purchase")
        .setDescription("Record a BOOST purchase: deduct balance + save history (admin)")
        .addUserOption(o => o.setName("user").setDescription("Buyer").setRequired(true))
        .addStringOption(o => o.setName("details").setDescription('e.g. "8 x +12"').setRequired(true))
        .addIntegerOption(o => o.setName("gold_cost").setDescription("Gold to deduct (1000000 = 1M)").setRequired(true))
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

    new SlashCommandBuilder()
        .setName("history")
        .setDescription("View your last 10 purchases"),

    new SlashCommandBuilder()
        .setName("historyuser")
        .setDescription("View a user's last 10 purchases (admin)")
        .addUserOption(o => o.setName("user").setDescription("Buyer").setRequired(true))
        .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),
].map(c => c.toJSON());

const dmCommands = commands.filter((c) => c.name === "me");

const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN);

(async () => {
    try {
        console.log("... Registering guild commands...");
        await rest.put(
            Routes.applicationGuildCommands(process.env.CLIENT_ID, process.env.GUILD_ID),
            { body: commands }
        );
        await rest.put(
            Routes.applicationCommands(process.env.CLIENT_ID),
            { body: dmCommands }
        );
        console.log("OK: Commands registered!");
    } catch (err) {
        console.error(err);
    }
})();
