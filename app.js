const ModbusRTU = require('modbus-serial');
const mqtt = require('mqtt');
const fs = require('fs');
const mysql = require('mysql2/promise');

// Hilfsfunktionen für Logging mit Zeitstempel und Level
function logError(message, ...args) {
  const timestamp = new Date().toISOString();
  console.error(`[${timestamp}] ERROR: ${message}`, ...args);
}

function logInfo(message, ...args) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] INFO: ${message}`, ...args);
}

function logDebug(message, ...args) {
  const timestamp = new Date().toISOString();
  // console.debug(`[${timestamp}] DEBUG: ${message}`, ...args);
}

// Konfigurationsdatei laden
function loadConfig() {
  try {
    const data = fs.readFileSync('modbus_addresses.json');
    return JSON.parse(data);
  } catch (error) {
    logError('Fehler beim Laden der JSON-Konfiguration:', error.message);
    process.exit(1);
  }
}

const { config, addresses: modbusAddresses } = loadConfig();
const {
  modbus_host: MODBUS_HOST,
  modbus_port: MODBUS_PORT,
  mqtt_broker: MQTT_BROKER,
  mqtt_topic: MQTT_TOPIC,
  mqtt_alarm_topic: MQTT_ALARM_TOPIC,
  mqtt_status_topic: MQTT_STATUS_TOPIC,
  mqtt_username: MQTT_USERNAME,
  mqtt_password: MQTT_PASSWORD,
  max_registers_per_request: MAX_REGISTERS_PER_REQUEST,
  polling_interval: POLLING_INTERVAL,
  mariadb: MARIADB_CONFIG
} = config;

// MariaDB-Pool erstellen
const pool = mysql.createPool({
  host: MARIADB_CONFIG.host,
  user: MARIADB_CONFIG.user,
  password: MARIADB_CONFIG.password,
  database: MARIADB_CONFIG.database,
  waitForConnections: true,
  connectionLimit: MARIADB_CONFIG.connectionLimit,
  queueLimit: 0
});

const modbusClient = new ModbusRTU();
const mqttClient = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD
});

let readCycleCounter = 0;
let modbusConnected = false;
let reconnectTimeout = null;

// Funktion zur dynamischen Adress-Gruppierung ohne Filter
function groupAddressesDynamically() {
  const addresses = Object.keys(modbusAddresses)
    .map(Number)
    .sort((a, b) => a - b);

  logDebug(`Anzahl der Modbus-Adressen: ${addresses.length}`);

  if (addresses.length === 0) {
    logInfo('Keine Modbus-Adressen zum Lesen vorhanden.');
    return [];
  }

  const groupedAddresses = [];
  let currentGroup = [addresses[0]];

  for (let i = 1; i < addresses.length; i++) {
    const prevAddress = addresses[i - 1];
    const currentAddress = addresses[i];

    if (currentAddress - prevAddress > 1 || currentGroup.length >= MAX_REGISTERS_PER_REQUEST) {
      groupedAddresses.push(currentGroup);
      currentGroup = [];
    }
    currentGroup.push(currentAddress);
  }
  if (currentGroup.length > 0) groupedAddresses.push(currentGroup);
  return groupedAddresses;
}

// Alarmstatus aus der Datenbank abrufen
async function getAlarmStatus() {
  let connection = null;
  try {
    connection = await pool.getConnection();
    const [result] = await connection.query(`
      SELECT 
        COUNT(*) as totalActive,
        SUM(CASE WHEN priority = 'prio1' THEN 1 ELSE 0 END) as prio1,
        SUM(CASE WHEN priority = 'prio2' THEN 1 ELSE 0 END) as prio2,
        SUM(CASE WHEN priority = 'prio3' THEN 1 ELSE 0 END) as prio3,
        SUM(CASE WHEN priority = 'warnung' THEN 1 ELSE 0 END) as warnung,
        SUM(CASE WHEN priority = 'info' THEN 1 ELSE 0 END) as info
      FROM alarms
    `);
    return result[0];
  } catch (error) {
    logError('Fehler bei der Abfrage des Alarmstatus:', error.message);
    throw error;
  } finally {
    if (connection) connection.release();
  }
}

// Funktion zum Verbindungsaufbau mit dem Modbus-Server
async function connectModbus() {
  try {
    await modbusClient.connectTCP(MODBUS_HOST, { port: MODBUS_PORT });
    modbusClient.setID(1);
    modbusConnected = true;
    logInfo('Erfolgreich mit dem Modbus-Server verbunden.');
  } catch (error) {
    modbusConnected = false;
    logError('Fehler beim Verbindungsaufbau zum Modbus-Server:', error.message);
    scheduleReconnect();
  }
}

// Funktion zum Planen eines erneuten Verbindungsversuchs
function scheduleReconnect() {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }
  reconnectTimeout = setTimeout(() => {
    logInfo('Versuche, die Verbindung zum Modbus-Server erneut herzustellen...');
    connectModbus();
  }, 10000); // 10 Sekunden warten
}

// Modbus-Daten lesen und senden
async function readModbusDataWithCycle() {
  if (!modbusConnected) {
    logInfo('Modbus-Server nicht verbunden. Warte auf nächsten Verbindungsversuch.');
    return;
  }

  const payload = [];
  const alarmPayload = [];

  const groupedAddresses = groupAddressesDynamically();

  if (groupedAddresses.length === 0) {
    logInfo('Keine Modbus-Adressen zum Lesen in diesem Zyklus.');
    readCycleCounter++;
    return;
  }

  let readingSuccessful = true;

  try {
    for (const group of groupedAddresses) {
      const startAddress = group[0];
      const count = group[group.length - 1] - startAddress + 1;

      try {
        const data = await modbusClient.readHoldingRegisters(startAddress, count);

        group.forEach((address, index) => {
          const entry = modbusAddresses[address];
          const value = data.data[address - startAddress] * entry.factor;

          const dataPoint = {
            address: address,
            value: value,
            topic: entry.topic,
            type: entry.type,
            gw: entry.gw,
            log: entry.log,
            alarm: entry.alarm,
            settings: entry.settings,
            qhmi: entry.qhmi,
            hkl: entry.hkl,
            factor: entry.factor
            // timestamp: Date.now()
          };

          payload.push(dataPoint);

          if (entry.alarm) {
            alarmPayload.push(dataPoint);
          }
        });

        logDebug(`Modbus-Daten gelesen von Adresse ${startAddress} bis ${startAddress + count - 1}`);

      } catch (err) {
        logError(`Fehler beim Lesen des Modbus-Blocks ab Adresse ${startAddress}:`, err.message);
        readingSuccessful = false;
        modbusConnected = false;
        modbusClient.close();
        scheduleReconnect();
        break; // Bei Fehler Abbruch des Lesevorgangs
      }
    }

    if (readingSuccessful) {
      // Daten an MQTT senden
      if (payload.length > 0) {
        mqttClient.publish(MQTT_TOPIC, JSON.stringify(payload, null, 2));
        logInfo(`Gesamtzahl der gesendeten Datenpunkte: ${payload.length} an Topic: ${MQTT_TOPIC}`);
      } else {
        logInfo('Keine Datenpunkte zum Senden an MQTT.');
      }

      if (alarmPayload.length > 0) {
        mqttClient.publish(MQTT_ALARM_TOPIC, JSON.stringify(alarmPayload, null, 2));
        logInfo(`Alarm-Daten gesendet (${alarmPayload.length} Einträge) an Topic: ${MQTT_ALARM_TOPIC}`);
      } else {
        logInfo("Keine Alarm-Daten zum Senden.");
      }

      // Alarmstatus abrufen und senden
      try {
        const status = await getAlarmStatus();
        mqttClient.publish(MQTT_STATUS_TOPIC, JSON.stringify(status));
        logInfo(`Alarmstatus gesendet an Topic ${MQTT_STATUS_TOPIC}:`, status);
      } catch (error) {
        logError('Fehler beim Senden des Alarmstatus:', error.message);
      }
    } else {
      logError('Modbus-Lesevorgang fehlgeschlagen. MQTT-Nachrichten werden nicht gesendet.');
    }

  } catch (error) {
    logError('Allgemeiner Fehler beim Lesen der Modbus-Daten:', error.message);
    modbusConnected = false;
    modbusClient.close();
    scheduleReconnect();
  } finally {
    readCycleCounter++;
  }
}

// Hauptfunktion
function main() {
  connectModbus(); // Initiale Verbindung herstellen
  setInterval(readModbusDataWithCycle, POLLING_INTERVAL);
  logInfo('Modbus-MQTT Gateway gestartet.');
}

main();
