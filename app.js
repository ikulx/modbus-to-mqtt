const ModbusRTU = require('modbus-serial');
const mqtt = require('mqtt');
const fs = require('fs');

// Modbus- und MQTT-Konfiguration laden
function loadConfig() {
  const data = fs.readFileSync('/app/modbus_addresses.json');
  const configData = JSON.parse(data);
  return configData;
}

const { config, addresses: modbusAddresses } = loadConfig();
const {
  modbus_host: MODBUS_HOST,
  modbus_port: MODBUS_PORT,
  mqtt_broker: MQTT_BROKER,
  mqtt_topic: MQTT_TOPIC,
  mqtt_alarm_topic: MQTT_ALARM_TOPIC, // Alarm-Topic laden
  mqtt_username: MQTT_USERNAME,
  mqtt_password: MQTT_PASSWORD,
  max_registers_per_request: MAX_REGISTERS_PER_REQUEST,
  polling_interval: POLLING_INTERVAL
} = config;

const modbusClient = new ModbusRTU();
const mqttClient = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD
});

// Zähler für den 5-Zyklus für _R_ Topics
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
    
    // Neuen Block starten, wenn der Abstand zu groß ist oder die maximale Anzahl erreicht wird
    if (currentAddress - prevAddress > 1 || currentGroup.length >= MAX_REGISTERS_PER_REQUEST) {
      groupedAddresses.push(currentGroup);
      currentGroup = [];
    }
    currentGroup.push(currentAddress);
  }
  if (currentGroup.length > 0) groupedAddresses.push(currentGroup);
  return groupedAddresses;
}

// Funktion zum Lesen der Modbus-Daten mit Berücksichtigung des 5-Zyklus
async function readModbusDataWithCycle() {
  const payload = [];
  const alarmPayload = []; // Separate Payload für Alarme
  
  // Nur alle 5 Zyklen _R_-Topics einbeziehen
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

          // Haupt-Payload zusammenstellen
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

          // Wenn `alarm: true`, zur Alarm-Payload hinzufügen
          if (entry.alarm) {
            alarmPayload.push({
              address: address,
              value: value,
              topic: entry.topic,
              gw: entry.gw,
            });
          }
        });
      } catch (err) {
        console.error(`Fehler beim Lesen des Modbus-Blocks ab Adresse ${startAddress}:`, err);
      }
    }

    // Haupt-Payload an Standard-MQTT-Topic senden
    mqttClient.publish(MQTT_TOPIC, JSON.stringify(payload, null, 2));
    console.log(`Daten erfolgreich an MQTT gesendet: ${MQTT_TOPIC}`);
    
    // Falls Alarme vorhanden, sende sie an das separate Alarm-Topic
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
    readCycleCounter++; // Zykluszähler erhöhen
  }
}

// Hauptfunktion mit dynamischem Intervall
function main() {
  setInterval(readModbusDataWithCycle, POLLING_INTERVAL); // Verwendung des Intervalls aus der JSON-Datei
}

main();
