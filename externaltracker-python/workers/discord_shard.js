require("dotenv").config(
    {
        quiet: true
    }
);

const {
    Client,
    GatewayIntentBits
} = require("discord.js");

const redis = require("redis");


const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = process.env.REDIS_PORT || "6379";
const RAW_QUEUE_NAME = process.env.PROVIO_RAW_MESSAGES_QUEUE || "provio_raw_messages_queue";
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;

const TARGET_CHANNEL_IDS = (
    process.env.DISCORD_TARGET_CHANNEL_IDS || ""
)
    .split(",")
    .map(function (
        channelId
    ) {
        return channelId.trim();
    })
    .filter(function (
        channelId
    ) {
        return channelId.length > 0;
    });


if (!DISCORD_BOT_TOKEN) {
    throw new Error(
        "DISCORD_BOT_TOKEN is missing in .env"
    );
}


const redisClient = redis.createClient(
    {
        url: `redis://${REDIS_HOST}:${REDIS_PORT}`,
        socket: {
            reconnectStrategy: false
        }
    }
);

let redisConnected = false;


redisClient.on(
    "error",
    function (
        error
    ) {
        if (!redisConnected) {
            return;
        }

        console.error(
            "[DISCORD-REDIS-ERROR]",
            error
        );
    }
);


const client = new Client(
    {
        intents: [
            GatewayIntentBits.Guilds,
            GatewayIntentBits.GuildMessages,
            GatewayIntentBits.MessageContent
        ]
    }
);


client.once(
    "ready",
    function () {
        console.log(
            `[DISCORD-SHARD] Logged in as ${client.user.tag}`
        );

        if (TARGET_CHANNEL_IDS.length > 0) {
            console.log(
                `[DISCORD-SHARD] Listening only to channel IDs: ${TARGET_CHANNEL_IDS.join(", ")}`
            );
        } else {
            console.log(
                "[DISCORD-SHARD] Listening to all readable channels."
            );
        }
    }
);


client.on(
    "messageCreate",
    async function (
        message
    ) {
        try {
            if (message.author && message.author.bot) {
                return;
            }

            if (
                TARGET_CHANNEL_IDS.length > 0 &&
                !TARGET_CHANNEL_IDS.includes(
                    message.channel.id
                )
            ) {
                return;
            }

            const mediaUrls = Array.from(
                message.attachments.values()
            ).map(function (
                attachment
            ) {
                return attachment.url;
            });

            const rawText = message.content || "";

            if (
                rawText.trim().length === 0 &&
                mediaUrls.length === 0
            ) {
                return;
            }

            const payload = {
                source: "discord",
                channel: message.channel ? message.channel.name : null,
                channel_id: message.channel ? message.channel.id : null,
                guild: message.guild ? message.guild.name : null,
                guild_id: message.guild ? message.guild.id : null,
                message_id: message.id,
                raw_text: rawText,
                timestamp: new Date(
                    message.createdTimestamp || Date.now()
                ).toISOString(),
                media_urls: mediaUrls
            };

            await redisClient.rPush(
                RAW_QUEUE_NAME,
                JSON.stringify(
                    payload
                )
            );

            console.log(
                `[DISCORD-WORKER] Captured message ${message.id} from #${payload.channel}`
            );
        } catch (error) {
            console.error(
                "[DISCORD-WORKER-ERROR]",
                error
            );
        }
    }
);


async function startDiscordShard() {
    await redisClient.connect();
    redisConnected = true;

    console.log(
        `[DISCORD-SHARD] Connected to Redis at ${REDIS_HOST}:${REDIS_PORT}.`
    );

    await client.login(
        DISCORD_BOT_TOKEN
    );
}


startDiscordShard().catch(
    function (
        error
    ) {
        console.error(
            "[DISCORD-SHARD-STARTUP-ERROR]",
            error
        );

        process.exit(
            1
        );
    }
);
