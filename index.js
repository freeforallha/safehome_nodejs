// 🔥 CORE
const fs = require("fs");
const mqtt = require("mqtt");
const admin = require("firebase-admin");
const crypto = require("crypto");

const lastHeartbeatMap = {};
const lastAlarmMap = {};
const lastNotificationMap = {};
const lastScheduleAlarmMap = {};
const pendingEventAlarmMap = {};
const pendingEventAlarmTimerMap = {};

function getPiSerial() {
  try {
    const cpuInfo = fs.readFileSync("/proc/cpuinfo", "utf8");
    const match = cpuInfo.match(/Serial\s*:\s*(.+)/);

    if (match && match[1]) {
      return match[1].trim();
    }

    return "unknown_serial";
  } catch (err) {
    console.log("CPU INFO ERROR:", err);
    return "unknown_serial";
  }
}

const rawId = getPiSerial();

const DEVICE_ID =
  "dev_" +
  crypto.createHash("sha256").update(rawId).digest("hex").slice(0, 16);

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

// ================= TIME =================
function formatDateTime(ts) {
  const d = new Date(ts);
  const pad = (n) => n.toString().padStart(2, "0");

  return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(
    d.getHours(),
  )}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function getCurrentHHMM() {
  const now = new Date();

  const hh = now.getHours().toString().padStart(2, "0");
  const mm = now.getMinutes().toString().padStart(2, "0");

  return `${hh}:${mm}`;
}

function toMin(t) {
  const [h, m] = (t || "00:00").split(":").map(Number);
  return h * 60 + m;
}

function isNowInRange(startTime, endTime) {
  const now = new Date();
  const nowMin = now.getHours() * 60 + now.getMinutes();

  const start = toMin(startTime || "23:00");
  const end = toMin(endTime || "06:00");

  if (start > end) {
    return nowMin >= start || nowMin <= end;
  }

  return nowMin >= start && nowMin <= end;
}
function getNextAlarmTimeText(repeatMinutes) {
  const minutes = parseInt(repeatMinutes || 0);

  if (minutes <= 0) {
    return "không lặp lại";
  }

  const next = new Date(Date.now() + minutes * 60 * 1000);
  const hh = next.getHours().toString().padStart(2, "0");
  const mm = next.getMinutes().toString().padStart(2, "0");

  return `${hh}:${mm}`;
}
function getHomeSafety(home) {
  const devices = home.devices || {};
  const unsafeDevices = [];

  for (const [deviceId, device] of Object.entries(devices)) {
    const name = device.name || deviceId;
    const type = device.type || "door";

    if (type === "door") {
      if (device.contact === false) {
        unsafeDevices.push(`${name} đang mở`);
      }

      if (device.tamper === true) {
        unsafeDevices.push(`${name} bị tháo`);
      }

      continue;
    }

    if (type === "smoke") {
      if (device.smoke === true) {
        unsafeDevices.push(`${name} phát hiện khói`);
      }

      if (device.tamper === true) {
        unsafeDevices.push(`${name} bị tháo`);
      }

      continue;
    }

    if (type === "sos") {
      const lastTriggered = Number(device.last_triggered || 0);
      const isRecentlyTriggered = lastTriggered > 0 && Date.now() - lastTriggered < 60 * 1000;

      if (isRecentlyTriggered) {
        unsafeDevices.push(`${name} đã kích hoạt SOS`);
      }

      continue;
    }
  }

  return {
    safe: unsafeDevices.length === 0,
    unsafeDevices,
  };
}
function getDeviceTypeFromModel(modelId, description, ieee) {
  const id = (ieee || "").toLowerCase();
  const model = (modelId || "").toLowerCase();
  const desc = (description || "").toLowerCase();

  if (id === "0xa4c1388295d25926") return "smoke";
  if (id === "0xa4c138b872c891a2") return "temperature";
  if (id === "0xa4c1381162d4d15b") return "sos";
  if (id === "0xa4c13898b084dbdc") return "repeater";

  if (desc.includes("smoke") || model.includes("ts0205")) return "smoke";
  if (desc.includes("temperature") || desc.includes("humidity")) return "temperature";
  if (desc.includes("sos") || desc.includes("button")) return "sos";
  if (desc.includes("repeater") || model.includes("ts0207")) return "repeater";

  return "door";
}
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

    // console.log("🔄 DEVICE MAP:", Object.keys(deviceMap).length); 
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

// ================= PUSH =================
async function sendScheduledNotification(uid, homeId, text, isSafe, reason = "") {
  try {
    const now = Date.now();
    const key = `${uid}_${homeId}_${text}_${getCurrentHHMM()}`;

    // chống spam trong 70 giây
    if (
      lastNotificationMap[key] &&
      now - lastNotificationMap[key] < 70 * 1000
    ) {
      return;
    }

    lastNotificationMap[key] = now;

    const snap = await db.ref(`accounts/${uid}/fcmToken`).once("value");
    const token = snap.val();

    if (!token) {
      console.log("❌ NO TOKEN FOR SCHEDULE:", uid);
      return;
    }

    await admin.messaging().send({
      token,

      data: {
        type: "schedule_notification",
        title: "🏡 SAFEHOME",
        body: text || "",
        homeId: homeId || "",
        uid: uid || "",
        isSafe: isSafe ? "true" : "false",
        reason: reason || "",
        clickAction: "schedule_SCREEN",
      },

      android: {
        priority: "high",

        notification: {
          title: "🏡 SafeHome",
          body: text || "Nhắc nhở SafeHome",
          channelId: "safehome_schedule_fullscreen_channel",
          priority: "max",
        },
      },
    });

    console.log("🔔 SCHEDULE NOTIFICATION:", uid, homeId, text);
  } catch (err) {
    console.log("NOTIFICATION SEND ERROR:", err.message);
  }
}
async function canReceiveAlarm(uid, homeId) {
  try {
    const snap = await db
      .ref(`accounts/${uid}/alarmSettings/${homeId}/enabled`)
      .once("value");

    const enabled = snap.val();

    return enabled !== false;
  } catch (err) {
    console.log("ALARM SETTING CHECK ERROR:", err.message);
    return true;
  }
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
    const enabled = await canReceiveAlarm(uid, homeId);

    if (!enabled) {
      console.log("🔕 ALARM MUTED BY USER:", uid, homeId);
      return;
    }
    const snap = await db.ref(`accounts/${uid}/fcmToken`).once("value");
    const token = snap.val();

    if (!token) {
      console.log("❌ NO TOKEN");
      return;
    }

    await admin.messaging().send({
      token,

      data: {
        type: "alarm",
        title: "🚨 SAFEHOME",
        body: reason || "Có cảnh báo!",
        homeId: homeId || "",
        uid: uid || "",
        clickAction: "alarm_SCREEN",
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
async function sendAlarmSummary(uid, items) {
  try {
    if (!items || items.length === 0) {
      return;
    }
    const uniqueItems = [];

    for (const item of items) {
      const exists = uniqueItems.some((oldItem) => {
        return oldItem.homeId === item.homeId && oldItem.reason === item.reason;
      });

      if (!exists) {
        uniqueItems.push(item);
      }
    }

    items = uniqueItems;
    const firstHomeId = items[0].homeId;

    const enabled = await canReceiveAlarm(uid, firstHomeId);

    if (!enabled) {
      console.log("🔕 ALARM SUMMARY MUTED BY USER:", uid, firstHomeId);
      return;
    }

    const snap = await db.ref(`accounts/${uid}/fcmToken`).once("value");
    const token = snap.val();

    if (!token) {
      console.log("❌ NO TOKEN FOR ALARM SUMMARY:", uid);
      return;
    }

    const lines = items.slice(0, 4).map((item) => {
      return `${item.homeName}: ${item.reason}`;
    });

    if (items.length > 4) {
      lines.push("...");
    }

    await admin.messaging().send({
      token,

      data: {
        type: "alarm",
        title: "🚨 SAFEHOME",
        body: lines.join("\n"),
        alarmItems: JSON.stringify(items),
        clickAction: "alarm_SCREEN",
      },

      android: {
        priority: "high",
      },
    });

    console.log("🚨 SUMMARY PUSH SENT:", uid, items.length);
  } catch (err) {
    console.log("SUMMARY ALARM ERROR:", err.message);
  }
}
function queueEventAlarm(uid, item) {
  if (!pendingEventAlarmMap[uid]) {
    pendingEventAlarmMap[uid] = [];
  }

  const exists = pendingEventAlarmMap[uid].some((oldItem) => {
    return oldItem.homeId === item.homeId && oldItem.reason === item.reason;
  });

  if (!exists) {
    pendingEventAlarmMap[uid].push(item);
  }

  if (pendingEventAlarmTimerMap[uid]) {
    return;
  }

  pendingEventAlarmTimerMap[uid] = setTimeout(async () => {
    const items = pendingEventAlarmMap[uid] || [];

    delete pendingEventAlarmMap[uid];
    delete pendingEventAlarmTimerMap[uid];

    await sendAlarmSummary(uid, items);
  }, 1200);
}
// ================= SCHEDULE CHECK =================
async function checkScheduledNotifications() {
  try {
    const snap = await db.ref("accounts").once("value");
    const accounts = snap.val() || {};
    const current = getCurrentHHMM();

    console.log("⏰ CHECK SCHEDULE:", current);

    for (const [uid, user] of Object.entries(accounts)) {
      const homes = user.homes || {};

      for (const [homeId, home] of Object.entries(homes)) {
        const schedules = home.schedules || {};
        const notificationsRaw = schedules.notifications || {};
        const notifications = Array.isArray(notificationsRaw)
          ? notificationsRaw
          : Object.values(notificationsRaw);

        for (const item of notifications) {

          if (!item || item.enabled !== true) {
            continue;
          }

          if (item.time !== current) {
            continue;
          }

          const homeName = home.name || homeId;
          const safety = getHomeSafety(home);

          if (safety.safe) {
            await sendScheduledNotification(
              uid,
              homeId,
              "Nhà bạn đã an toàn, hãy an tâm đi ngủ.",
              true,
              ""
            );
          } else {
            const detail = safety.unsafeDevices.slice(0, 3).join(", ");

            await sendScheduledNotification(
              uid,
              homeId,
              `⚠️ Nhà ${homeName} chưa an toàn: ${detail}`,
              false,
              detail
            );
          }
        }
      }
    }
  } catch (err) {
    console.log("SCHEDULE CHECK ERROR:", err.message);
  }
}

// ================= DEVICE NOTIFICATION LOG =================
async function addDeviceNotification(
  uid,
  homeId,
  deviceId,
  text,
  type = "status",
) {
  try {
    const now = Date.now();

    const deviceRef = db.ref(
      `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
    );

    const notifRef = deviceRef.child(`notifications/${now}`);

    await notifRef.set({
      time: now,
      text,
      type,
    });

    const notifSnap = await deviceRef.child("notifications").once("value");
    const notif = notifSnap.val() || {};
    const keys = Object.keys(notif);

    if (keys.length > 100) {
      keys.sort();

      const keysToRemove = keys.slice(0, keys.length - 100);

      for (const key of keysToRemove) {
        await deviceRef.child(`notifications/${key}`).remove();
      }
    }

    console.log("📝 NOTIFICATION:", text);
  } catch (err) {
    console.log("NOTIFICATION ERROR:", err.message);
  }
}

// ================= ALARM LOGIC =================

async function processScheduleAlarmsForOwner(
  uid,
  homeId,
  homeName,
  deviceName,
  homeData,
  updateData,
) {
  const schedules = homeData.schedules || {};
  const alarms = schedules.alarms || [];

  for (const item of alarms) {
    if (!item || item.enabled !== true) continue;

    const inTime = isNowInRange(item.start, item.end);

    if (!inTime) continue;

    if (updateData.contact === false) {
      queueEventAlarm(uid, {
        homeId,
        homeName,
        reason: `${deviceName}: Cửa mở bất thường`,
        repeatMinutes: 0,
        nextAlarm: "theo lịch alarm đã cài",
      });
      return;
    }

    if (updateData.tamper === true) {
      queueEventAlarm(uid, {
        homeId,
        homeName,
        reason: `${deviceName}: Thiết bị bị tháo`,
        repeatMinutes: 0,
        nextAlarm: "theo lịch alarm đã cài",
      });
      return;
    }
  }
}
async function checkScheduledAlarms() {
  console.log("🚨 CHECK ALARM SCHEDULE");

  try {
    const snap = await db.ref("accounts").once("value");
    const accounts = snap.val() || {};

    const now = Date.now();
    const today = new Date().toDateString();

    const alarmSummaryByUser = {};

    for (const [uid, user] of Object.entries(accounts)) {
      const ownHomes = user.homes || {};
      const sharedHomes = user.sharedHomes || {};

      const homesToCheck = [];

      for (const [homeId, home] of Object.entries(ownHomes)) {
        homesToCheck.push({
          receiverUid: uid,
          ownerUid: uid,
          homeId,
          home,
          source: "owner",
        });
      }

      for (const [homeId, sharedInfo] of Object.entries(sharedHomes)) {
        const ownerUid = sharedInfo?.ownerUid;
        if (!ownerUid) continue;

        const homeSnap = await db
          .ref(`accounts/${ownerUid}/homes/${homeId}`)
          .once("value");

        const sharedHome = homeSnap.val();
        if (!sharedHome) continue;

        homesToCheck.push({
          receiverUid: uid,
          ownerUid,
          homeId,
          home: sharedHome,
          source: "shared",
        });
      }

      for (const item of homesToCheck) {
        const { receiverUid, ownerUid, homeId, home, source } = item;

        const canReceive = await canReceiveAlarm(receiverUid, homeId);
        if (!canReceive) {
          console.log("🔕 ALARM MUTED BY USER:", receiverUid, homeId);
          continue;
        }

        const schedules = home.schedules || {};
        const alarmsRaw = schedules.alarms || {};

        const alarms = Array.isArray(alarmsRaw)
          ? alarmsRaw
          : Object.values(alarmsRaw);

        const safety = getHomeSafety(home);

        console.log("🚨 ALARM HOME:", receiverUid, homeId, {
          ownerUid,
          source,
          safe: safety.safe,
          unsafeDevices: safety.unsafeDevices,
          alarms,
        });

        if (safety.safe) continue;

        for (const alarm of alarms) {
          if (!alarm || alarm.enabled !== true) continue;
          if (!isNowInRange(alarm.start, alarm.end)) continue;

          const repeatMinutes = parseInt(alarm.repeatMinutes || 0);
          const alarmKey = `${receiverUid}_${ownerUid}_${homeId}_${alarm.start}_${alarm.end}_${today}`;
          const lastTime = lastScheduleAlarmMap[alarmKey] || 0;

          if (repeatMinutes === 0 && lastTime > 0) continue;

          if (
            repeatMinutes > 0 &&
            lastTime > 0 &&
            now - lastTime < repeatMinutes * 60 * 1000
          ) {
            continue;
          }

          lastScheduleAlarmMap[alarmKey] = now;

          const detail = safety.unsafeDevices.slice(0, 3).join(", ");

          if (!alarmSummaryByUser[receiverUid]) {
            alarmSummaryByUser[receiverUid] = [];
          }

          alarmSummaryByUser[receiverUid].push({
            homeId,
            homeName: home.name || homeId,
            reason: detail,
            repeatMinutes,
            nextAlarm: getNextAlarmTimeText(repeatMinutes),
          });

          break;
        }
      }
    }

    for (const [receiverUid, items] of Object.entries(alarmSummaryByUser)) {
      await sendAlarmSummary(receiverUid, items);
    }
  } catch (err) {
    console.log("ALARM CHECK ERROR:", err.message);
  }
}
// ================= INIT =================
async function init() {
  startDeviceMapListener();

  await db.ref("pair_requests").remove();
  console.log("🧹 OLD PAIR REQUESTS CLEARED");

  setInterval(checkScheduledNotifications, 60000);
  setInterval(checkScheduledAlarms, 60000);
}

init();

// ================= MQTT CONNECT =================
client.on("connect", () => {
  console.log("MQTT CONNECTED");
  client.subscribe("zigbee2mqtt/#");
  client.subscribe("zigbee2mqtt/bridge/event");
});

// ================= PAIRING =================
db.ref("pair_requests").on("child_added", async (snap) => {
  const data = snap.val();
  const key = snap.key;

  if (!data) return;

  setTimeout(async () => {
    try {
      await db.ref(`pair_requests/${key}`).remove();
      console.log("🧹 PAIR REQUEST REMOVED:", key);
    } catch (e) {
      console.log("❌ REMOVE ERROR:", e.message);
    }
  }, (data.duration || 60) * 1000);

  if (data.active !== true) return;

  if ((data.hubId || "").trim() !== DEVICE_ID.trim()) return;

  if (pairingSession?.key === key) return;

  console.log("🟢 PAIR START:", key, data.homeId);

  if (!data?.requestedBy || !data?.homeId) {
    console.log("❌ INVALID PAIR REQUEST DATA", data);
    return;
  }

  pairingSession = {
    key,
    uid: data.requestedBy,
    homeId: data.homeId,
  };

  await setPermitJoin(true, data.duration || 60);

  setTimeout(async () => {
    await setPermitJoin(false);
    pairingSession = null;

    console.log("🧹 PAIR DONE:", key);
  }, (data.duration || 60) * 1000);
});

db.ref("pair_requests").on("child_removed", (snap) => {
  console.log("🧹 REQUEST REMOVED:", snap.key);
});

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

        const deviceType = getDeviceTypeFromModel(
          payload?.definition?.model,
          payload?.definition?.description,
          ieee,
        );
        let defaultName = "Thiết bị";

        const devicesSnap = await db
          .ref(`accounts/${uid}/homes/${homeId}/devices`)
          .once("value");

        const devices = devicesSnap.val() || {};

        const sameTypeCount = Object.values(devices).filter((d) => {
          return d?.type === deviceType;
        }).length + 1;

        switch (deviceType) {
          case "door":
            defaultName = `Cửa ${sameTypeCount}`;
            break;

          case "smoke":
            defaultName = `Báo cháy ${sameTypeCount}`;
            break;

          case "temperature":
            defaultName = `Nhiệt độ ${sameTypeCount}`;
            break;

          case "sos":
            defaultName = `SOS ${sameTypeCount}`;
            break;

          case "repeater":
            defaultName = `Bộ mở rộng sóng ${sameTypeCount}`;
            break;

          default:
            defaultName = `Thiết bị ${sameTypeCount}`;
        }
        await db.ref(`accounts/${uid}/homes/${homeId}/devices/${ieee}`).set({
          name: defaultName,
          ieee,
          type: deviceType,

          availability: "unknown",
          last_seen: null,

          battery: null,
          linkquality: null,

          contact: null,
          smoke: null,
          tamper: false,
          temperature: null,
          humidity: null,
          action: null,

          last_event: null,
          created: Date.now(),
          updated_at: Date.now(),
        });

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

    const rawTopic = topic.replace("zigbee2mqtt/", "");
    if (rawTopic.startsWith("bridge")) return;

    const topicParts = rawTopic.split("/");
    const deviceId = topicParts[0];
    const subTopic = topicParts[1] || null;

    if (subTopic === "availability") {
      const availabilityValue =
        typeof data === "string"
          ? data
          : data.state || data.availability || data.status || null;

      if (!availabilityValue) return;

      const map = deviceMap[deviceId];
      if (!map) return;

      const { uid, homeId } = map;

      await db
        .ref(`accounts/${uid}/homes/${homeId}/devices/${deviceId}`)
        .update({
          availability: availabilityValue,
          updated_at: Date.now(),
        });

      console.log("📶 AVAILABILITY:", deviceId, availabilityValue);
      return;
    }

    const map = deviceMap[deviceId];
    if (!map) return;

    const { uid, homeId } = map;

    const deviceRef = db.ref(
      `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
    );

    const oldSnap = await deviceRef.once("value");
    const oldData = oldSnap.val() || {};
    const deviceName = oldData.name || deviceId;

    const homeSnap = await db
      .ref(`accounts/${uid}/homes/${homeId}`)
      .once("value");

    const homeData = homeSnap.val() || {};
    const homeName = homeData.name || homeId;

    const oldTamper = oldData.tamper;

    const now = Date.now();

    let updateData = {};
    updateData.updated_at = now;
    if (data.availability !== undefined) {
      updateData.availability = data.availability;
    }

    if (data.last_seen !== undefined) {
      updateData.last_seen = data.last_seen;
    }
    if (data.linkquality !== undefined) {
      updateData.linkquality = data.linkquality;
    }
    if (data.contact !== undefined) {
      updateData.contact = data.contact;

      if (data.contact !== oldData.contact) {
        updateData.last_event = now;
      }
    }

    if (data.tamper !== undefined) {
      const newTamper = data.tamper;
      updateData.tamper = newTamper;

      if (newTamper !== oldTamper) {
        updateData.last_event = now;
      }
    }

    if (data.battery !== undefined) {
      updateData.battery = data.battery;
      updateData.battery_status = "percent";
    }

    if (data.battery_low !== undefined) {
      updateData.battery_low = data.battery_low;
      updateData.battery_status = data.battery_low === true ? "low" : "ok";
    }
    if (data.smoke !== undefined) {
      updateData.type = "smoke";
      updateData.smoke = data.smoke;

      if (data.smoke !== oldData.smoke) {
        updateData.last_event = now;
      }
    }

    if (data.temperature !== undefined) {
      updateData.type = "temperature";
      updateData.temperature = data.temperature;
    }

    if (data.humidity !== undefined) {
      updateData.type = "temperature";
      updateData.humidity = data.humidity;
    }

    if (data.action !== undefined) {
      updateData.type = "sos";
      updateData.action = data.action;
      updateData.last_event = now;
      updateData.last_triggered = now;
      updateData.sos_active_until = now + 5 * 60 * 1000;
    }


    await deviceRef.update(updateData);


    if (updateData.last_event !== undefined) {
      let statusText = "";

      const currentType = updateData.type || oldData.type || "door";

      if (currentType === "door") {
        statusText = updateData.contact === false ? "Cửa mở" : "Cửa đóng";
      } else if (currentType === "smoke") {
        statusText = updateData.smoke === true ? "Phát hiện khói" : "Khói đã trở lại bình thường";
      } else if (currentType === "sos") {
        statusText = "Nút SOS đã được bấm";
      } else if (currentType === "temperature") {
        statusText = "Cập nhật nhiệt độ / độ ẩm";
      } else if (currentType === "repeater") {
        statusText = "Bộ mở rộng sóng đã cập nhật trạng thái";
      } else {
        statusText = "Thiết bị đã cập nhật trạng thái";
      }

      await addDeviceNotification(
        uid,
        homeId,
        deviceId,
        statusText,
        "status",
      );
    }

    if (updateData.tamper !== undefined && updateData.tamper !== oldTamper) {
      await addDeviceNotification(
        uid,
        homeId,
        deviceId,
        updateData.tamper ? "Tamper detected" : "Tamper cleared",
        "tamper",
      );
    }

    console.log("📡 UPDATE:", deviceId, updateData);

    // ===== NEW MULTI ALARM FORMAT =====
    await processScheduleAlarmsForOwner(
      uid,
      homeId,
      homeName,
      deviceName,
      homeData,
      updateData,
    );

    // ===== SHARED USERS ALARM FROM OWNER HOME SCHEDULE =====
    const sharedSnap = await db.ref(`sharedByHome/${homeId}`).once("value");
    const sharedMap = sharedSnap.val() || {};
    console.log("🚨 SHARED ALARM USERS:", homeId, sharedMap);

    for (const sharedUid of Object.keys(sharedMap)) {
      const sharedHomeSnap = await db
        .ref(`accounts/${sharedUid}/sharedHomes/${homeId}`)
        .once("value");

      const sharedHomeInfo = sharedHomeSnap.val() || {};

      if (sharedHomeInfo.alarmEnabled === false) {
        continue;
      }

      await processScheduleAlarmsForOwner(
        sharedUid,
        homeId,
        homeName,
        deviceName,
        homeData,
        updateData,
      );
    }
  } catch (err) {
    console.log("MQTT ERROR:", err.message);
  }
});