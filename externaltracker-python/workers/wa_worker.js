require("dotenv").config(
    {
        quiet: true
    }
);

const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason
} = require("@whiskeysockets/baileys");

const qrcode = require("qrcode-terminal");
const redis = require("redis");


const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = process.env.REDIS_PORT || "6379";
const RAW_QUEUE_NAME = process.env.PROVIO_RAW_MESSAGES_QUEUE || "provio_raw_messages_queue";
const WHATSAPP_SESSION_PATH = process.env.WHATSAPP_SESSION_PATH || "./wa_session";

const silentLogger = {
    level: "silent",
    child: function () {
        return this;
    },
    trace: function () {},
    debug: function () {},
    info: function () {},
    warn: function () {},
    error: function () {},
    fatal: function () {}
};


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
            "[WA-REDIS-ERROR]",
            error
        );
    }
);


async function startWhatsAppWorker() {
    if (!redisClient.isOpen) {
        await redisClient.connect();
        redisConnected = true;

        console.log(
            "[WA-WORKER] Connected to Redis."
        );
    }

    const {
        state,
        saveCreds
    } = await useMultiFileAuthState(
        WHATSAPP_SESSION_PATH
    );

    const sock = makeWASocket(
        {
            auth: state,
            logger: silentLogger,
            syncFullHistory: false,
            markOnlineOnConnect: false,
            printQRInTerminal: false
        }
    );

    sock.ev.on(
        "connection.update",
        function (
            update
        ) {
            const {
                connection,
                lastDisconnect,
                qr
            } = update;

            if (qr) {
                console.log(
                    "[WA-WORKER] Scan this QR code with WhatsApp:"
                );

                qrcode.generate(
                    qr,
                    {
                        small: true
                    }
                );
            }

            if (connection === "close") {
                const statusCode = lastDisconnect
                    ?.error
                    ?.output
                    ?.statusCode;

                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

                console.error(
                    "[WA-WORKER] Connection closed. Reconnecting:",
                    shouldReconnect
                );

                if (shouldReconnect) {
                    startWhatsAppWorker();
                } else {
                    console.error(
                        "[WA-WORKER] Logged out. Delete wa_session and scan again."
                    );
                }
            }

            if (connection === "open") {
                console.log(
                    "[WA-WORKER] Connected and listening."
                );
            }
        }
    );

    sock.ev.on(
        "creds.update",
        saveCreds
    );

    sock.ev.on(
        "messages.upsert",
        async function (
            messageUpdate
        ) {
            try {
                if (
                    messageUpdate.type &&
                    messageUpdate.type !== "notify"
                ) {
                    return;
                }

                const messages = messageUpdate.messages || [];

                for (const msg of messages) {
                    if (!msg.message) {
                        continue;
                    }

                    if (msg.key && msg.key.fromMe) {
                        continue;
                    }

                    const rawText =
                        msg.message.conversation ||
                        msg.message.extendedTextMessage?.text ||
                        msg.message.imageMessage?.caption ||
                        msg.message.videoMessage?.caption ||
                        "";

                    if (!rawText.trim()) {
                        continue;
                    }

                    const payload = {
                        source: "whatsapp",
                        channel: msg.key.remoteJid || null,
                        message_id: msg.key.id || null,
                        raw_text: rawText,
                        timestamp: new Date().toISOString(),
                        media_urls: []
                    };

                    await redisClient.rPush(
                        RAW_QUEUE_NAME,
                        JSON.stringify(
                            payload
                        )
                    );

                    console.log(
                        `[WA-WORKER] Captured message: ${payload.message_id}`
                    );
                }
            } catch (error) {
                console.error(
                    "[WA-WORKER-MESSAGE-ERROR]",
                    error
                );
            }
        }
    );
}


setInterval(
    function () {
        console.log(
            "[WA-HEALTH] Worker heartbeat OK"
        );
    },
    60000
);


startWhatsAppWorker().catch(
    function (
        error
    ) {
        console.error(
            "[WA-WORKER-STARTUP-ERROR]",
            error
        );

        process.exit(
            1
        );
    }
);
