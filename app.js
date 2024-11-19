const ModbusRTU = require('modbus-serial');
const mqtt = require('mqtt');
const fs = require('fs');
const mysql = require('mysql2/promise');

// Konfigurationsdatei laden
function loadConfig() {
  try {
    const data = fs.readFileSync('modbus_addresses.json');
    return JSON.parse(data);
  } catch (error) {
    console.error('Fehler beim Laden der JSON-Konfiguration:', error.message);
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
  mqtt_status_topic: MQTT_STATUS_TOPIC, // Neues Status-Topic
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

// Funktion zur dynamischen Adress-Gruppierung
function groupAddressesDynamically(onlyNonRTopics = false) {
  const addresses = Object.keys(modbusAddresses)
    .map(Number)
    .filter(address => !onlyNonRTopics || !modbusAddresses[address].topic.includes("_R_"))
    .sort((a, b) => a - b);

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

// Modbus-Daten lesen und senden
async function readModbusDataWithCycle() {
  const payload = [];
  const alarmPayload = [];

  const includeRTopics = readCycleCounter % 5 === 0;
  const groupedAddresses = groupAddressesDynamically(!includeRTopics);

  try {
    await modbusClient.connectTCP(MODBUS_HOST, { port: MODBUS_PORT });
    modbusClient.setID(1);

    for (const group of groupedAddresses) {
      const startAddress = group[0];
      const count = group[group.length - 1] - startAddress + 1;

      try {
        const data = await modbusClient.readHoldingRegisters(startAddress, count);

        group.forEach((address, index) => {
          const entry = modbusAddresses[address];
          const value = data.data[address - startAddress] * entry.factor;

          payload.push({
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
            factor: entry.factor,
            timestamp: Date.now()
          });

          if (entry.alarm) {
            alarmPayload.push({
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
              factor: entry.factor,
              timestamp: Date.now()
            });
          }
        });
      } catch (err) {
        console.error(`Fehler beim Lesen des Modbus-Blocks ab Adresse ${startAddress}:`, err);
      }
    }

    mqttClient.publish(MQTT_TOPIC, JSON.stringify(payload, null, 2));
    console.log(`Daten erfolgreich an MQTT gesendet: ${MQTT_TOPIC}`);

    if (alarmPayload.length > 0) {
      mqttClient.publish(MQTT_ALARM_TOPIC, JSON.stringify(alarmPayload, null, 2));
      console.log(`Alarm-Daten erfolgreich an MQTT gesendet: ${MQTT_ALARM_TOPIC}`);
    } else {
      console.log("Keine Alarme zum Senden.");
    }

  } catch (error) {
    console.error('Fehler beim Lesen der Modbus-Daten:', error);
  } finally {
    modbusClient.close();
    readCycleCounter++;
  }
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
    console.error('Fehler bei der Abfrage des Alarmstatus:', error);
    throw error;
  } finally {
    if (connection) connection.release();
  }
}

// Status periodisch senden
function startAlarmStatusService() {
  setInterval(async () => {
    try {
      const status = await getAlarmStatus();
      mqttClient.publish(MQTT_STATUS_TOPIC, JSON.stringify(status)); // An Status-Topic senden
      console.log(`Status erfolgreich gesendet an ${MQTT_STATUS_TOPIC}:`, status);
    } catch (error) {
      console.error('Fehler beim Senden des Status:', error);
    }
  }, 5000);
}

// Hauptfunktion
function main() {
  setInterval(readModbusDataWithCycle, POLLING_INTERVAL);
  startAlarmStatusService();
}

main();
