require("dotenv").config(
    {
        quiet: true
    }
);

const path = require("path");
const { ShardingManager } = require("discord.js");


const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN;


if (!DISCORD_BOT_TOKEN) {
    throw new Error(
        "DISCORD_BOT_TOKEN is missing in .env"
    );
}


const shardFilePath = path.join(
    __dirname,
    "discord_shard.js"
);


const manager = new ShardingManager(
    shardFilePath,
    {
        token: DISCORD_BOT_TOKEN,
        totalShards: "auto"
    }
);


manager.on(
    "shardCreate",
    function (
        shard
    ) {
        console.log(
            `[DISCORD-MANAGER] Launched shard ${shard.id}`
        );
    }
);


manager.spawn();
