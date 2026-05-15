// 🔥 CORE
const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const admin = require("firebase-admin");
const { machineIdSync } = require("node-machine-id");
const crypto = require("crypto");
const rawId = machineIdSync();
const lastHeartbeatMap = {};
const DEVICE_ID =
  "dev_" + crypto.createHash("sha256").update(rawId).digest("hex").slice(0, 16);

console.log("🧠 DEVICE_ID:", DEVICE_ID);

// ================= FIREBASE =================
const serviceAccount = require("./serviceAccount.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL:
    "https://safehome-10cc9-default-rtdb.asia-southeast1.firebasedatabase.app/",
});
const db = admin.database();

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
  startDeviceMapListener();

  setInterval(() => { }, 30000);
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

  if (!data?.active) return;

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
    timeoutId,
  };
});
// ================= ALARM PUSH =================
const lastAlarmMap = {};
function formatDateTime(ts) {
  const d = new Date(ts);

  const pad = (n) => n.toString().padStart(2, "0");

  return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
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

      notification: {
        title: "🚨 CẢNH BÁO",
        body: reason || "Có xâm nhập!",
      },

      data: {
        type: "alarm",
        homeId: homeId || "",
        uid: uid || "",
        clickAction: "alarm_SCREEN",
      },

      android: {
        priority: "high",

        notification: {
          channelId: "alarm_channel",
          sound: "default",
          priority: "max",
        },
      },
    });

    console.log("🚨 PUSH SENT:", uid, homeId);
  } catch (err) {
    console.log("FCM ERROR:", err.message);
  }
}
// ================= DEVICE NOTIFICATION =================
async function addDeviceNotification(
  uid,
  homeId,
  deviceId,
  text,
  type = "status",
) {
  try {
    const ref = db.ref(
      `accounts/${uid}/homes/${homeId}/devices/${deviceId}/notifications/${Date.now()}`,
    );

    await ref.set({
      time: Date.now(),
      text,
      type,
    });
    const notifSnap = await deviceRef.child("notifications").once("value");

    const notif = notifSnap.val() || {};

    const keys = Object.keys(notif);

    if (keys.length > 100) {
      keys.sort();

      await deviceRef
        .child(`notifications/${keys[0]}`)
        .remove();
    }

    console.log("📝 NOTIFICATION:", text);
  } catch (err) {
    console.log("NOTIFICATION ERROR:", err.message);
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

        // ================= CHECK DEVICE ĐÃ TỒN TẠI CHƯA =================
        const snap = await db
          .ref(`system/devices_by_ieee/${ieee}`)
          .once("value");

        const existing = snap.val();

        if (existing?.uid && existing?.homeId) {
          const oldRef = db.ref(
            `accounts/${existing.uid}/homes/${existing.homeId}/devices/${ieee}`,
          );

          const snapOld = await oldRef.once("value");

          if (snapOld.exists()) {
            await oldRef.remove();
          }
        }

        // ================= ADD DEVICE VÀO HOME MỚI =================
        await db.ref(`accounts/${uid}/homes/${homeId}/devices/${ieee}`).set({
          name: payload.friendly_name || ieee,
          ieee,
          status: "unknown",
          tamper: false,
          battery: 100,
          last_seen: Date.now(),
          created: Date.now(),
        });

        // ================= UPDATE GLOBAL INDEX =================
        await db.ref(`system/devices_by_ieee/${ieee}`).set({
          uid,
          homeId,
          deviceId: DEVICE_ID,
          hubType: "raspberry_pi",
          updatedAt: Date.now(),
        });

        deviceMap[ieee] = { uid, homeId };

        console.log("✅ DEVICE READY:", ieee);
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

    const deviceRef = db.ref(
      `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
    );

    const oldSnap = await deviceRef.once("value");
    const oldData = oldSnap.val() || {};

    const oldStatus = oldData.status;
    const oldTamper = oldData.tamper;

    let updateData = {
      last_seen: Date.now(),
    };
    if (data.linkquality !== undefined) {
      updateData.linkquality = data.linkquality;
    }
    updateData.last_seen_text = formatDateTime(Date.now());
    if (data.linkquality !== undefined) {
      if (data.linkquality < 50) {
        updateData.signal_status = "weak";
      } else if (data.linkquality < 100) {
        updateData.signal_status = "medium";
      } else {
        updateData.signal_status = "strong";
      }
    }
    if (data.contact !== undefined) {
      updateData.status = data.contact ? "closed" : "open";
    }

    if (data.tamper !== undefined) {
      updateData.tamper = data.tamper;
    }

    if (data.battery !== undefined) {
      updateData.battery = data.battery;
    }

    await deviceRef.update(updateData);
    // ================= HEARTBEAT LOG =================
    const heartbeatKey = `${uid}_${homeId}_${deviceId}`;

    const now = Date.now();

    if (
      !lastHeartbeatMap[heartbeatKey] ||
      now - lastHeartbeatMap[heartbeatKey] > 300000
    ) {
      lastHeartbeatMap[heartbeatKey] = now;

      await addDeviceNotification(
        uid,
        homeId,
        deviceId,
        `Cập nhật| ${(updateData.status ?? oldData.status) === "closed"
          ? "Đóng"
          : "Mở"
        } | ${(updateData.tamper ?? oldData.tamper)
          ? "Bị tháo"
          : "Bình thường"
        } | Pin: ${updateData.battery ?? oldData.battery ?? "?"}% | Tín hiệu: ${updateData.linkquality ?? oldData.linkquality ?? "?"}`,
        "heartbeat",
      );
    }

    // ================= STATUS NOTIFICATION =================
    if (
      updateData.status !== undefined &&
      updateData.status !== oldStatus
    ) {
      await addDeviceNotification(
        uid,
        homeId,
        deviceId,
        updateData.status === "open"
          ? "Door opened"
          : "Door closed",
        "status",
      );
    }

    // ================= TAMPER NOTIFICATION =================
    if (
      updateData.tamper !== undefined &&
      updateData.tamper !== oldTamper
    ) {
      await addDeviceNotification(
        uid,
        homeId,
        deviceId,
        updateData.tamper
          ? "Tamper detected"
          : "Tamper cleared",
        "tamper",
      );
    }

    console.log("📡 UPDATE:", deviceId, updateData);
    // ================= CHECK ALARM REALTIME =================
    const alarmSnap = await db.ref(`accounts/${uid}/homes/${homeId}/alarm`).once("value");
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
