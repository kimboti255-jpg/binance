const express = require('express');
const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const PORT = process.env.PORT || 7860;

app.use(express.urlencoded({ extended: true }));

// 1. Database ya SQLite ndani ya /tmp (Tumebadilisha jina kuwa binance_walls.db ili isigongane na faili lingine)
const dbPath = path.join('/tmp', 'binance_walls.db');
const db = new sqlite3.Database(dbPath, (err) => {
    if (err) console.error("Hitilafu ya SQLite DB:", err.message);
    else console.log("SQLite Database imeunganishwa vizuri kwenye /tmp/binance_walls.db.");
});

db.serialize(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS order_walls (
            id TEXT PRIMARY KEY,
            type TEXT,
            price REAL,
            amount REAL,
            value REAL,
            first_seen INTEGER,
            last_seen INTEGER
        )
    `);
});

// 2. Kuunganisha Binance WebSocket
let currentPair = "dogeusdt";
let ws;

function connectWebSocket(pair) {
    if (ws) {
        try { ws.removeAllListeners(); ws.close(); } catch (e) {}
    }

    const endpoint = wss://stream.binance.com:9443/ws/${pair}@depth@100ms;
    ws = new WebSocket(endpoint);

    ws.on('open', () => {
        console.log(`Muunganisho wa Binance WebSocket umefanikiwa! Pair: ${pair}`);
    });

    ws.on('message', (data) => {
        try {
            let jsonData = JSON.parse(data);
            let bidsData = jsonData.b || [];
            let asksData = jsonData.a || [];

            const now = Date.now();

            function processOrders(orders, type) {
                orders.forEach((order) => {
                    let bei = Number(order[0]);
                    let kiasi = Number(order[1]);
                    let thamani = bei * kiasi;
                    let elementId = `${type}-${bei}`;

                    if (thamani >= 1000000) {
                        db.run(`
                            INSERT INTO order_walls (id, type, price, amount, value, first_seen, last_seen)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(id) DO UPDATE SET
                                amount = excluded.amount,
                                value = excluded.value,
                                last_seen = excluded.last_seen
                        `, [elementId, type, bei, kiasi, thamani, now, now]);
                    } else {
                        db.run(`DELETE FROM order_walls WHERE id = ?`, [elementId]);
                    }
                });
            }

            processOrders(bidsData, "bid");
            processOrders(asksData, "ask");

        } catch (error) {
            console.error("Error processing message:", error.message);
        }
    });

    ws.on('error', (err) => console.error("WS Error:", err.message));
    ws.on('close', () => {
        console.log("WS Closed. Reconnecting...");
        setTimeout(() => connectWebSocket(currentPair), 3000);
    });
}

connectWebSocket(currentPair);

function formatAmount(amount) {
    if (amount >= 1000000) return "$" + (amount / 1000000).toFixed(2).replace(/\.00$/, "").replace(/(\.[1-9])0$/, "$1") + "M";
    if (amount >= 1000) return "$" + Math.round(amount / 1000) + "K";
    return "$" + Math.round(amount);
}

// Ujumbe wa ukurasa wa mwanzo ili kuondoa hitilafu ya Cannot GET /
app.get('/', (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Binance Order Walls</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body { font-family: Arial, sans-serif; max-width: 500px; margin: 40px auto; padding: 0 16px; }
                input { width: 100%; padding: 10px; font-size: 16px; box-sizing: border-box; margin-bottom: 10px; }
                button { width: 100%; padding: 12px; font-size: 16px; cursor: pointer; background: #f0b90b; border: none; border-radius: 4px; font-weight: bold; }
                .info { margin-top: 20px; font-size: 14px; color: #555; }
                .pair { font-weight: bold; color: #000; }
            </style>
        </head>
        <body>
            <h2>Binance Order Walls</h2>
            <p>Pair ya sasa: <span class="pair">${currentPair.toUpperCase()}</span></p>
            <form method="POST" action="/set-pair">
                <input type="text" name="pair" placeholder="Weka pair (mfano: btcusdt)" required>
                <button type="submit">Badilisha Pair</button>
            </form>
            <div class="info">
                <a href="/json-data">Angalia /json-data</a>
            </div>
        </body>
        </html>
    `);
});

app.post('/set-pair', (req, res) => {
    let newPair = (req.body.pair || '').trim().toLowerCase();
    newPair = newPair.replace(/[^a-z0-9]/g, '');

    if (!newPair) {
        return res.status(400).send('Pair si sahihi. Rudi nyuma na ujaribu tena.');
    }

    currentPair = newPair;

    db.run(`DELETE FROM order_walls`, [], (err) => {
        if (err) console.error("Hitilafu kufuta order_walls:", err.message);
    });

    connectWebSocket(currentPair);

    res.redirect('/');
});

// 3. API Endpoint /json-data
app.get('/json-data', (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');

    const now = Date.now();

    db.all("SELECT * FROM order_walls ORDER BY price DESC", [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });

        let bids = [];
        let asks = [];

        rows.forEach(row => {
            let durationMinutes = Number(((now - row.first_seen) / (1000 * 60)).toFixed(1));

            let orderData = {
                id: row.id,
                type: row.type,
                price: row.price,
                amount: row.amount,
                value: row.value,
                value_formatted: formatAmount(row.value),
                duration_minutes: durationMinutes,
                first_seen_iso: new Date(row.first_seen).toISOString()
            };

            if (row.type === 'bid') bids.push(orderData);
            else asks.push(orderData);
        });

        asks.sort((a, b) => a.price - b.price);

        res.json({
            status: "success",
            timestamp: new Date().toISOString(),
            bids_count: bids.length,
            asks_count: asks.length,
            bids: bids,
            asks: asks
        });
    });
});

app.listen(PORT, () => console.log(`Binance Server inakimbia kwenye port ${PORT}`));