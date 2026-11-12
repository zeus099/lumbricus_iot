const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const db = new sqlite3.Database('/data/lumbricus.db', (err) => {
    if (err) console.error('Gagal membuka DB:', err.message);
    else console.log('Terhubung ke database SQLite.');
});

db.serialize(() => {
    db.run("PRAGMA journal_mode = WAL;");
    db.run(`CREATE TABLE IF NOT EXISTS sensor_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        t1 REAL, t2 REAL, t3 REAL, t_avg REAL,
        m1 INTEGER, m2 INTEGER, m3 INTEGER, m_avg REAL,
        ph REAL, level REAL, valve REAL,
        pump_irigasi INTEGER, state TEXT,
        safe_soil INTEGER, safe_temp INTEGER
    )`);
    const newCols = [
        "pump_em4 INTEGER", "pump_molase INTEGER", "pump_air INTEGER", "pump_dist INTEGER",
        "nutrisi_siap INTEGER", "emergency INTEGER",
        "ovr_irigasi INTEGER", "ovr_molase INTEGER", "ovr_em4 INTEGER",
        "ovr_air INTEGER", "ovr_dist INTEGER", "ovr_valve INTEGER"
    ];
    newCols.forEach(col => {
        db.run(`ALTER TABLE sensor_logs ADD COLUMN ${col}`, (err) => {
            if (err && !/duplicate column/i.test(err.message)) console.warn(err.message);
        });
    });
});

app.use(express.static('public'));
app.use(express.json());

app.get('/api/export-csv', (req, res) => {
    const limit = req.query.limit || 10000;
    const query = `SELECT * FROM sensor_logs ORDER BY id DESC LIMIT ?`;
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="lumbricus_logs_${Date.now()}.csv"`);
    res.write("ID,Timestamp,Temp1,Temp2,Temp3,TempAvg,Moist1,Moist2,Moist3,MoistAvg,pH,Level,Valve,Pump_Trigasi,State\n");
    db.each(query, [limit], (err, row) => {
        if (err) return;
        res.write(`${row.id},"${row.timestamp}",${row.t1},${row.t2},${row.t3},${row.t_avg},${row.m1},${row.m2},${row.m3},${row.m_avg},${row.ph},${row.level},${row.valve},${row.pump_irigasi === 1 ? 'ON' : 'OFF'},"${row.state}"\n`);
    }, () => res.end());
});

app.get('/api/logs', (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 15;
    const offset = (page - 1) * limit;
    db.get(`SELECT COUNT(*) as total FROM sensor_logs`, [], (err, countResult) => {
        if (err) return res.status(500).json({ error: err.message });
        db.all(`SELECT * FROM sensor_logs ORDER BY id DESC LIMIT ? OFFSET ?`, [limit, offset], (err, rows) => {
            if (err) return res.status(500).json({ error: err.message });
            res.json({ total: countResult.total, page: page, totalPages: Math.ceil(countResult.total / limit), data: rows });
        });
    });
});

const MQTT_BROKER_URL = 'mqtt://192.168.1.13:1883';
const mqttClient = mqtt.connect(MQTT_BROKER_URL);

// State default lengkap dengan durasi
let scheduleState = {
    enabled: false, days: 0x7F, hour: 8, minute: 0, auto_empty: true,
    dur_molase: 670, dur_em4: 8, dur_air: 33, dur_dist: 20, dur_ferm: 172800
};

mqttClient.on('connect', () => {
    console.log('Backend terhubung ke Mosquitto Broker.');
    mqttClient.subscribe(['lumbricus/data', 'lumbricus/schedule']);
});

mqttClient.on('message', (topic, message) => {
    if (topic === 'lumbricus/schedule') {
        try {
            scheduleState = JSON.parse(message.toString());
            io.emit('scheduleData', scheduleState);
        } catch (e) { console.error('Schedule JSON error:', e.message); }
        return;
    }

    if (topic === 'lumbricus/data') {
        try {
            const data = JSON.parse(message.toString());
            const stmt = db.prepare(`INSERT INTO sensor_logs 
                (timestamp, t1, t2, t3, t_avg, m1, m2, m3, m_avg, ph, level, valve, pump_irigasi, state, safe_soil, safe_temp,
                 pump_em4, pump_molase, pump_air, pump_dist, nutrisi_siap, emergency,
                 ovr_irigasi, ovr_molase, ovr_em4, ovr_air, ovr_dist, ovr_valve) 
                VALUES (datetime('now','localtime'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

            stmt.run([
                data.t1, data.t2, data.t3, data.t_avg, data.m1, data.m2, data.m3, data.m_avg,
                data.ph, data.level, data.valve, data.pump_irigasi, data.state,
                data.safe_soil, data.safe_temp, data.pump_em4, data.pump_molase,
                data.pump_air, data.pump_dist, data.nutrisi_siap, data.emergency,
                data.ovr_irigasi, data.ovr_molase, data.ovr_em4, data.ovr_air, data.ovr_dist, data.ovr_valve
            ], (err) => { if (err) console.error('DB Insert Error:', err.message); });
            stmt.finalize();

            io.emit('mqttData', data);
        } catch (e) { console.error('JSON Error:', e.message); }
    }
});

io.on('connection', (socket) => {
    // 1. Ambil riwayat untuk grafik
    db.all(`SELECT timestamp, t_avg, m_avg, ph, level FROM sensor_logs ORDER BY id DESC LIMIT 30`, [], (err, rows) => {
        if (!err && rows) socket.emit('initialHistory', rows.reverse());
    });

    // 2. [BARU] Ambil 1 data terakhir dari database jika sistem sedang offline
    db.get(`SELECT * FROM sensor_logs ORDER BY id DESC LIMIT 1`, [], (err, row) => {
        if (!err && row) {
            socket.emit('mqttData', row); // Kirim data terakhir ke frontend
        }
    });

    socket.emit('scheduleData', scheduleState);

    socket.on('set_schedule', (cfg) => {
        mqttClient.publish('lumbricus/cmd', JSON.stringify({ cmd: 'SET_SCHEDULE', ...cfg }));
    });
    socket.on('trigger_now', () => {
        mqttClient.publish('lumbricus/cmd', JSON.stringify({ cmd: 'TRIGGER_NOW' }));
    });
    socket.on('reset_nutrisi', () => {
        mqttClient.publish('lumbricus/cmd', JSON.stringify({ cmd: 'RESET_NUTRISI' }));
    });
    socket.on('send_command', (commandObj) => {
        mqttClient.publish('lumbricus/cmd', JSON.stringify(commandObj));
    });
});

server.listen(3004, () => {
    console.log('Server Web Dashboard berjalan di http://localhost:3004');
});