// 🔥 CORE
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const admin = require("firebase-admin");

// ================= FIREBASE =================
const serviceAccount = require("./serviceAccount.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL:
    "https://safehome-10cc9-default-rtdb.asia-southeast1.firebasedatabase.app/",
});
const db = admin.database();
// ================= HUB ID (FIXED) =================
const { machineIdSync } = require("node-machine-id");

let HUB_ID = null;

async function ensureHubId() {
  try {
    const id = machineIdSync();

    HUB_ID = "hub_" + id.substring(0, 12);

    console.log("✅ FIXED HUB_ID:", HUB_ID);

    return HUB_ID;
  } catch (err) {
    console.log("❌ HUB_ID ERROR:", err.message);

    HUB_ID = "hub_unknown";
    return HUB_ID;
  }
}
// ================= REGISTER HUB =================
async function registerHub() {
  await db.ref(`system/hubs/${HUB_ID}`).set({
    created: Date.now(),
    last_seen: Date.now(),
    status: "online",
  });

  console.log("📡 HUB REGISTERED:", HUB_ID);
}

// ================= MQTT =================
const client = mqtt.connect("mqtt://localhost:1883");

// ================= DEVICE MAP =================
let deviceMap = {};
let pairingSession = null;

// ================= DEVICE LISTENER =================
function startDeviceMapListener() {
  db.ref("accounts").on("value", (snap) => {
    const data = snap.val() || {};
    const newMap = {};

    Object.entries(data).forEach(([uid, user]) => {
      const homes = user.homes || {};

      Object.entries(homes).forEach(([homeId, home]) => {
        const devices = home.devices || {};

        Object.entries(devices).forEach(([deviceId]) => {
          newMap[deviceId] = { uid, homeId };
        });
      });
    });

    deviceMap = newMap;

    console.log("🔄 DEVICE MAP:", Object.keys(deviceMap).length);
  });
}

// ================= PERMIT JOIN =================
function setPermitJoin(enable, time = 60) {
  return new Promise((resolve) => {
    client.publish(
      "zigbee2mqtt/bridge/request/permit_join",
      JSON.stringify({ value: enable, time }),
      () => {
        console.log("permit_join =", enable);
        resolve();
      },
    );
  });
}

// ================= INIT =================
async function init() {
  await ensureHubId();

  console.log("🚀 HUB:", HUB_ID);

  await registerHub();

  startDeviceMapListener();

  setInterval(() => {
    db.ref(`system/hubs/${HUB_ID}/last_seen`).set(Date.now());
  }, 30000);
}

init();

// ================= MQTT CONNECT =================
client.on("connect", () => {
  console.log("MQTT CONNECTED");
  client.subscribe("zigbee2mqtt/#");
  client.subscribe("zigbee2mqtt/bridge/event");
});

// ================= PAIRING =================
db.ref("system/pairing").on("value", async (snap) => {
  const data = snap.val();

  if (!data?.active) {
    if (pairingSession?.timeoutId) clearTimeout(pairingSession.timeoutId);
    pairingSession = null;
    return;
  }

  if (data.hubId !== HUB_ID) return;
  if (pairingSession) return;

  console.log("🟢 PAIRING START:", data.homeId);

  await setPermitJoin(true, data.duration || 60);

  const timeoutId = setTimeout(
    async () => {
      await setPermitJoin(false);
      await db.ref("system/pairing").set(null);

      pairingSession = null;

      console.log("🔴 PAIRING END");
    },
    (data.duration || 60) * 1000,
  );

  pairingSession = {
    uid: data.requestedBy,
    homeId: data.homeId,
    hubId: data.hubId,
    timeoutId,
  };
});
// ================= ALARM PUSH =================
const lastAlarmMap = {};

async function sendAlarm(uid, homeId, reason) {
  try {
    const now = Date.now();
    const key = `${uid}_${homeId}`;

    // chống spam trong 15s
    if (lastAlarmMap[key] && now - lastAlarmMap[key] < 15000) {
      return;
    }
    lastAlarmMap[key] = now;

    const snap = await db.ref(`accounts/${uid}/fcmToken`).once("value");
    const token = snap.val();

    if (!token) {
      console.log("❌ NO TOKEN");
      return;
    }

    await admin.messaging().send({
      token: token,

      data: {
        type: "ALARM",
        title: "🚨 CẢNH BÁO",
        body: reason || "Có xâm nhập!",
        homeId: homeId || "",
        uid: uid || "",
        clickAction: "ALARM_SCREEN",
      },

      android: {
        priority: "high",
      },
    });

    console.log("🚨 PUSH SENT:", uid, homeId);
  } catch (err) {
    console.log("FCM ERROR:", err.message);
  }
}

// ================= MQTT HANDLER =================
client.on("message", async (topic, msg) => {
  try {
    const data = JSON.parse(msg.toString());

    // ===== DEVICE JOIN =====
    if (topic === "zigbee2mqtt/bridge/event") {
      const type = data?.type;
      const payload = data?.data;

      if (!payload || !pairingSession) return;

      if (
        type === "device_announce" ||
        type === "device_interview" ||
        type === "device_connected"
      ) {
        const ieee = payload.ieee_address;
        if (!ieee) return;

        const { uid, homeId } = pairingSession;

        await db.ref(`accounts/${uid}/homes/${homeId}/devices/${ieee}`).set({
          name: payload.friendly_name || ieee,
          ieee,
          status: "unknown",
          tamper: false,
          battery: 100,
          last_seen: Date.now(),
          created: Date.now(),
        });

        deviceMap[ieee] = { uid, homeId };

        console.log("✅ DEVICE ADDED:", ieee);
        return;
      }
    }

    // ===== SENSOR UPDATE =====
    if (!topic.startsWith("zigbee2mqtt/")) return;

    const deviceId = topic.replace("zigbee2mqtt/", "");
    if (deviceId.startsWith("bridge")) return;

    const map = deviceMap[deviceId];
    if (!map) return;

    const { uid, homeId } = map;

    let updateData = {
      last_seen: Date.now(),
    };

    if (data.contact !== undefined) {
      updateData.status = data.contact ? "closed" : "open";
    }

    if (data.tamper !== undefined) {
      updateData.tamper = data.tamper;
    }

    if (data.battery !== undefined) {
      updateData.battery = data.battery;
    }

    await db
      .ref(`accounts/${uid}/homes/${homeId}/devices/${deviceId}`)
      .update(updateData);

    console.log("📡 UPDATE:", deviceId, updateData);
    // ================= CHECK ALARM REALTIME =================
    const alarmSnap = await db.ref(`accounts/${uid}/alarm`).once("value");
    const alarm = alarmSnap.val() || {};

    if (alarm.enabled) {
      function toMin(t) {
        const [h, m] = t.split(":").map(Number);
        return h * 60 + m;
      }

      const now = new Date();
      const nowMin = now.getHours() * 60 + now.getMinutes();

      const start = toMin(alarm.start || "23:00");
      const end = toMin(alarm.end || "06:00");

      let inTime = false;

      if (start > end) {
        inTime = nowMin >= start || nowMin <= end;
      } else {
        inTime = nowMin >= start && nowMin <= end;
      }
      console.log("⏰ ALARM CHECK:", updateData, "inTime:", inTime);

      if (!inTime) return;

      // ===== CHECK NGUY HIỂM =====
      if (updateData.status && updateData.status !== "closed") {
        await sendAlarm(uid, homeId, "Cửa chưa đóng");
      }

      if (updateData.tamper === true) {
        await sendAlarm(uid, homeId, "Phát hiện tháo thiết bị");
      }
    }
  } catch (err) {
    console.log("MQTT ERROR:", err.message);
  }
});
