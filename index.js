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
const pendingScheduleReminderMap = {};
const pendingScheduleReminderTimerMap = {};
const userDirectoryCache = {};
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

    // smoke và sos không tham gia alarm theo lịch
    // chúng đã được xử lý bằng emergency alarm realtime
    if (type === "smoke") continue;
    if (type === "sos") continue;


  }

  return {
    safe: unsafeDevices.length === 0,
    unsafeDevices,
  };
}
function getHeartbeatLimitMs(type) {
  if (type === "temperature") return 2 * 60 * 60 * 1000;
  if (type === "repeater") return 1 * 60 * 60 * 1000;
  if (type === "smoke") return 24 * 60 * 60 * 1000;
  if (type === "sos") return 6 * 60 * 60 * 1000;

  return 6 * 60 * 60 * 1000;
}

function getHomeNotificationSafety(home) {
  const devices = home.devices || {};
  const unsafeDevices = [];

  for (const [deviceId, device] of Object.entries(devices)) {
    const name = device.name || deviceId;
    const type = device.type || "door";
    const issues = [];

    const lastSeen = device.last_seen
      ? new Date(device.last_seen).getTime()
      : 0;

    const limitMs = getHeartbeatLimitMs(type);
    const offline =
      !lastSeen || Date.now() - lastSeen > limitMs * 1.3;

    if (offline) issues.push("mất kết nối");

    const batteryLow =
      device.battery_low === true ||
      (
        device.battery !== undefined &&
        device.battery !== null &&
        Number(device.battery) <= 20
      );

    if (batteryLow) issues.push("pin yếu");

    if (type === "door") {
      if (device.contact === false) issues.push("đang mở");
      if (device.tamper === true) issues.push("bị tháo");
    }

    if (type === "smoke") {
      if (device.smoke === true) issues.push("phát hiện khói");
      if (device.tamper === true) issues.push("bị tháo");
    }

    if (type === "sos") {
      const lastTriggered = Number(device.last_triggered || 0);
      const isRecentlyTriggered =
        lastTriggered > 0 && Date.now() - lastTriggered < 60 * 1000;

      if (isRecentlyTriggered) issues.push("đã kích hoạt SOS");
    }

    if (issues.length > 0) {
      unsafeDevices.push(`${name}: ${issues.join(", ")}`);
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
function startUserDirectoryListener() {
  db.ref("accounts").on("value", async (snap) => {
    try {
      const accounts = snap.val() || {};
      const updates = {};
      const activeUids = new Set(Object.keys(accounts));

      for (const [uid, rawUser] of Object.entries(accounts)) {
        const user = rawUser || {};
        const profile = user.profile || {};

        const directoryData = {
          email: String(user.email || "")
            .trim()
            .toLowerCase(),

          name: String(
            profile.name ||
            user.name ||
            "",
          ).trim(),

          photoUrl: String(
            profile.photoUrl ||
            user.photoUrl ||
            "",
          ).trim(),
        };

        const signature = JSON.stringify(directoryData);

        if (userDirectoryCache[uid] === signature) {
          continue;
        }

        userDirectoryCache[uid] = signature;

        updates[`userDirectory/${uid}`] = {
          ...directoryData,
          updatedAt: Date.now(),
        };
      }

      for (const cachedUid of Object.keys(userDirectoryCache)) {
        if (activeUids.has(cachedUid)) {
          continue;
        }

        delete userDirectoryCache[cachedUid];
        updates[`userDirectory/${cachedUid}`] = null;
      }

      if (Object.keys(updates).length > 0) {
        await db.ref().update(updates);

        console.log(
          "👤 USER DIRECTORY SYNC:",
          Object.keys(updates).length,
        );
      }
    } catch (err) {
      console.log(
        "USER DIRECTORY SYNC ERROR:",
        err.message,
      );
    }
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
async function sendScheduledReminderSummary(uid, items) {
  try {
    if (!items || items.length === 0) return;

    const uniqueItems = [];

    for (const item of items) {
      const exists = uniqueItems.some((oldItem) => {
        return (
          oldItem.homeId === item.homeId &&
          oldItem.isSafe === item.isSafe &&
          oldItem.reason === item.reason
        );
      });

      if (!exists) {
        uniqueItems.push(item);
      }
    }

    if (uniqueItems.length === 0) return;

    const allSafe = uniqueItems.every(
      (item) => item.isSafe === true,
    );

    const unsafeItems = uniqueItems.filter(
      (item) => item.isSafe !== true,
    );

    const reminderItems = [];

    for (const item of uniqueItems) {
      const rawItems = Array.isArray(item.reminderItems)
        ? item.reminderItems
        : [];

      for (const reminderItem of rawItems) {
        if (!reminderItem) continue;

        const exists = reminderItems.some((oldItem) => {
          return oldItem.homeId === reminderItem.homeId;
        });

        if (!exists) {
          reminderItems.push(reminderItem);
        }
      }
    }

    const title =
      uniqueItems.length === 1
        ? uniqueItems[0].homeName || "Nhà"
        : "SafeHome Reminder";

    let body = "";

    if (uniqueItems.length === 1) {
      body = uniqueItems[0].text || "";
    } else if (allSafe) {
      body =
        `${uniqueItems.length} nhà đã an toàn. ` +
        `Hãy an tâm nghỉ ngơi.`;
    } else {
      body =
        `${unsafeItems.length}/${uniqueItems.length} nhà ` +
        `đang có vấn đề cần kiểm tra.`;
    }

    const reason = unsafeItems
      .map((item) => {
        const homeName = item.homeName || "Nhà";
        const detail =
          item.reason || "Có vấn đề cần kiểm tra";

        return `${homeName}: ${detail}`;
      })
      .join("\n");

    const tokenSnap = await db
      .ref(`accounts/${uid}/fcmToken`)
      .once("value");

    const token = tokenSnap.val();

    if (!token) {
      console.log(
        "❌ NO TOKEN FOR SCHEDULE:",
        uid,
      );
      return;
    }

    await admin.messaging().send({
      token,

      data: {
        type: "schedule_notification",
        title,
        body,
        homeId:
          uniqueItems.length === 1
            ? uniqueItems[0].homeId || ""
            : "",
        uid: uid || "",
        isSafe: allSafe ? "true" : "false",
        reason,
        reminderItems: JSON.stringify(reminderItems),
        clickAction: "schedule_SCREEN",
      },

      android: {
        priority: "high",
      },
    });

    console.log(
      "🔔 SCHEDULE SUMMARY SENT:",
      uid,
      uniqueItems.length,
    );
  } catch (err) {
    console.log(
      "SCHEDULE SUMMARY ERROR:",
      err.message,
    );
  }
}

function queueScheduledReminder(uid, item) {
  if (!pendingScheduleReminderMap[uid]) {
    pendingScheduleReminderMap[uid] = [];
  }

  pendingScheduleReminderMap[uid].push(item);

  if (pendingScheduleReminderTimerMap[uid]) {
    return;
  }

  pendingScheduleReminderTimerMap[uid] = setTimeout(
    async () => {
      const items =
        pendingScheduleReminderMap[uid] || [];

      delete pendingScheduleReminderMap[uid];
      delete pendingScheduleReminderTimerMap[uid];

      await sendScheduledReminderSummary(
        uid,
        items,
      );
    },
    8000,
  );
}

async function sendScheduledNotification(
  uid,
  homeId,
  homeName,
  text,
  isSafe,
  reason = "",
  reminderItems = [],
) {
  try {
    const now = Date.now();
    const key =
      `${uid}_${homeId}_${text}_${getCurrentHHMM()}`;

    if (
      lastNotificationMap[key] &&
      now - lastNotificationMap[key] < 70 * 1000
    ) {
      return;
    }

    lastNotificationMap[key] = now;

    await addHomeNotificationFromBackend({
      uid,
      homeId,
      homeName,
      type: "reminder_triggered",
      category: "reminder",
      severity: isSafe ? "success" : "warning",
      title: isSafe
        ? "Reminder: Nhà đã an toàn"
        : "Reminder: Cần kiểm tra",
      message: isSafe
        ? "Nhà đang an toàn. Hãy an tâm nghỉ ngơi."
        : `Cần kiểm tra: ${reason ||
        "Nhà đang có vấn đề cần chú ý."
        }`,
      entityType: "home",
      entityId: homeId,
    });

    queueScheduledReminder(uid, {
      homeId,
      homeName,
      text,
      isSafe,
      reason,
      reminderItems,
    });
  } catch (err) {
    console.log(
      "NOTIFICATION SEND ERROR:",
      err.message,
    );
  }
}
function getTodayKey() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");

  return `${yyyy}-${mm}-${dd}`;
}

function isTimeInPauseRange(startTime, endTime) {
  if (!startTime || !endTime) return false;
  return isNowInRange(startTime, endTime);
}

async function isHomeAlarmPausedToday(ownerUid, homeId) {
  try {
    const snap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
      .once("value");

    const pause = snap.val();

    if (!pause) return false;

    const today = getTodayKey();

    if (pause.date !== today) {
      try {
        await db
          .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
          .remove();
      } catch (_) { }

      return false;
    }

    const paused = isTimeInPauseRange(
      pause.start,
      pause.end,
    );

    if (paused) {
      return true;
    }

    const now = getCurrentHHMM();

    const start = toMin(pause.start);
    const end = toMin(pause.end);
    const current = toMin(now);

    let pauseFinished = false;

    if (start > end) {
      pauseFinished =
        current > end &&
        current < start;
    } else {
      pauseFinished =
        current > end;
    }

    if (pauseFinished) {
      try {
        await db
          .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
          .remove();

        console.log(
          "🧹 ALARM PAUSE REMOVED:",
          ownerUid,
          homeId,
        );
      } catch (_) { }
    }

    return false;
  } catch (err) {
    console.log("ALARM PAUSE CHECK ERROR:", err.message);
    return false;
  }
}

async function canReceiveAlarm(uid, homeId, ownerUid = uid) {
  try {
    const settingSnap = await db
      .ref(`accounts/${uid}/alarmSettings/${homeId}/enabled`)
      .once("value");

    const enabled = settingSnap.val();

    // Mặc định là bật, chỉ tắt khi user chủ động set false
    if (enabled === false) {
      return false;
    }

    const paused = await isHomeAlarmPausedToday(ownerUid, homeId);

    if (paused) {
      console.log("⏸️ HOME ALARM PAUSED TODAY:", ownerUid, homeId);
      return false;
    }

    return true;
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

      apns: {
        headers: {
          "apns-priority": "10",
        },
        payload: {
          aps: {
            alert: {
              title: "🚨 SAFEHOME",
              body: reason || "Có cảnh báo!",
            },
            sound: "default",
            badge: 1,
          },
        },
      },
    });

    console.log("🚨 PUSH SENT:", uid, homeId);
  } catch (err) {
    console.log("FCM ERROR:", err.message);
  }
}
async function sendAlarmPauseNotification(
  uid,
  homeId,
  homeName,
  text,
) {
  try {
    const snap = await db
      .ref(`accounts/${uid}/fcmToken`)
      .once("value");

    const token = snap.val();

    if (!token) {
      return;
    }

    await admin.messaging().send({
      token,

      data: {
        type: "schedule_notification",
        forceShow: "true",
        reason: text,
        severity: "warning",
        isSafe: "false",
        title: homeName || "Nhà",
        body: text,
        homeId: homeId || "",
        uid: uid || "",
        clickAction: "schedule_SCREEN",
      },

      android: {
        priority: "high",
        notification: {
          title: homeName || "Nhà",
          body: text,
          channelId: "safehome_schedule_fullscreen_channel",
        },
      },
    });

    console.log(
      "⏸️ ALARM PAUSE WARNING SENT:",
      uid,
      homeId,
    );
  } catch (err) {
    console.log(
      "ALARM PAUSE NOTIFICATION ERROR:",
      err.message,
    );
  }
}
async function addHomeNotificationFromBackend({
  uid,
  homeId,
  homeName,
  type,
  title,
  message,
  category = "home",
  severity = "info",
  entityType = "home",
  entityId = "",
}) {
  try {
    if (!uid || !homeId) {
      return;
    }

    const now = Date.now();
    const resolvedHomeName =
      String(homeName || "").trim() || homeId;

    const listRef = db.ref(
      `accounts/${uid}/notifications`,
    );

    const notificationRef = listRef.push();

    await notificationRef.set({
      id: notificationRef.key,
      type,
      category,
      severity,
      title,
      message,
      homeId,
      homeName: resolvedHomeName,
      entityType,
      entityId: entityId || homeId,
      data: {
        homeName: resolvedHomeName,
      },
      time: now,
      read: false,
    });

    const snap = await listRef
      .orderByChild("time")
      .once("value");

    const notifications = snap.val() || {};
    const entries = Object.entries(notifications);

    if (entries.length > 120) {
      entries.sort((a, b) => {
        return (
          Number(a[1]?.time || 0) -
          Number(b[1]?.time || 0)
        );
      });

      const updates = {};
      const removeCount = entries.length - 120;

      for (const [key] of entries.slice(0, removeCount)) {
        updates[key] = null;
      }

      await listRef.update(updates);
    }

    console.log(
      "🏠 HOME NOTIFICATION:",
      uid,
      type,
      homeId,
    );
  } catch (err) {
    console.log(
      "HOME NOTIFICATION ERROR:",
      err.message,
    );
  }
}
async function sendAlarmSummary(uid, items) {
  try {
    if (!items || items.length === 0) return;

    const uniqueItems = [];

    for (const item of items) {
      const exists = uniqueItems.some((oldItem) => {
        return oldItem.homeId === item.homeId && oldItem.reason === item.reason;
      });

      if (!exists) uniqueItems.push(item);
    }

    const allowedItems = [];

    for (const item of uniqueItems) {
      const enabled = await canReceiveAlarm(
        uid,
        item.homeId,
        item.ownerUid || uid,
      );

      if (enabled) {
        allowedItems.push(item);
      }
    }

    if (allowedItems.length === 0) {
      console.log("🔕 ALARM SUMMARY MUTED ALL:", uid);
      return;
    }

    const snap = await db.ref(`accounts/${uid}/fcmToken`).once("value");
    const token = snap.val();

    if (!token) {
      console.log("❌ NO TOKEN FOR ALARM SUMMARY:", uid);
      return;
    }

    const lines = allowedItems.slice(0, 4).map((item) => {
      return `${item.homeName}: ${item.reason}`;
    });

    if (allowedItems.length > 4) {
      lines.push("...");
    }

    console.log("🚨 ALARM ITEMS:", JSON.stringify(allowedItems, null, 2));

    await admin.messaging().send({
      token,

      data: {
        type: "alarm",
        title: "🚨 SAFEHOME",
        body: lines.join("\n"),
        alarmItems: JSON.stringify(allowedItems),
        clickAction: "alarm_SCREEN",
      },

      android: {
        priority: "high",
      },

      apns: {
        headers: {
          "apns-priority": "10",
        },
        payload: {
          aps: {
            alert: {
              title: "🚨 SAFEHOME",
              body: lines.join("\n"),
            },
            sound: "default",
            badge: 1,
          },
        },
      },
    });

    console.log("🚨 SUMMARY PUSH SENT:", uid, allowedItems.length);
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
async function cleanupExpiredAlarmPause() {
  try {
    const snap = await db.ref("accounts").once("value");

    const accounts = snap.val() || {};

    const today = getTodayKey();
    const now = getCurrentHHMM();

    for (const [uid, user] of Object.entries(accounts)) {
      const homes = user.homes || {};

      for (const [homeId, home] of Object.entries(homes)) {
        const pause = home.alarmPauseToday;

        if (!pause) continue;

        if (pause.date !== today) {
          await db
            .ref(`accounts/${uid}/homes/${homeId}/alarmPauseToday`)
            .remove();

          const sharedSnap = await db
            .ref(`sharedByHome/${homeId}`)
            .once("value");

          const sharedUsers = sharedSnap.val() || {};

          for (const sharedUid of Object.keys(sharedUsers)) {
            await db
              .ref(
                `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`,
              )
              .remove();
          }

          console.log("🧹 OLD ALARM PAUSE REMOVED:", uid, homeId);

          continue;
        }

        if (
          !isTimeInPauseRange(
            pause.start,
            pause.end,
          )
        ) {
          const start = toMin(pause.start);
          const end = toMin(pause.end);
          const current = toMin(now);

          let finished = false;

          if (start > end) {
            finished =
              current > end &&
              current < start;
          } else {
            finished =
              current > end;
          }

          if (finished) {
            await db
              .ref(`accounts/${uid}/homes/${homeId}/alarmPauseToday`)
              .remove();

            const sharedSnap = await db
              .ref(`sharedByHome/${homeId}`)
              .once("value");

            const sharedUsers = sharedSnap.val() || {};

            for (const sharedUid of Object.keys(sharedUsers)) {
              await db
                .ref(
                  `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`,
                )
                .remove();
            }

            console.log(
              "🧹 EXPIRED ALARM PAUSE REMOVED:",
              uid,
              homeId,
            );
          }
        }
      }
    }
  } catch (err) {
    console.log(
      "ALARM PAUSE CLEANUP ERROR:",
      err.message,
    );
  }
}
async function checkScheduledNotifications() {
  try {
    const snap = await db.ref("accounts").once("value");
    const accounts = snap.val() || {};
    const current = getCurrentHHMM();


    console.log("⏰ CHECK SCHEDULE:", current);

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
        const { receiverUid, homeId, home, source } = item;

        let notifications = [];

        if (source === "shared") {
          const customSnap = await db
            .ref(`accounts/${receiverUid}/customRules/${homeId}/notifications/items`)
            .once("value");

          const customRaw = customSnap.val() || {};

          notifications = Array.isArray(customRaw)
            ? customRaw
            : Object.values(customRaw);
        } else {
          const schedules = home.schedules || {};
          const notificationsRaw = schedules.notifications || {};

          notifications = Array.isArray(notificationsRaw)
            ? notificationsRaw
            : Object.values(notificationsRaw);
        }

        for (const item of notifications) {
          console.log(
            "🔎 REMINDER DEBUG:",
            receiverUid,
            homeId,
            source,
            JSON.stringify(item),
            "CURRENT:",
            current,
          );

          if (!item || item.enabled !== true) continue;
          if (String(item.time || "").trim() !== current) continue;

          const homeName = home.name || homeId;
          const safety = getHomeNotificationSafety(home);

          if (safety.safe) {
            await sendScheduledNotification(
              receiverUid,
              homeId,
              homeName,
              `Nhà bạn đã an toàn, hãy an tâm đi ngủ.

Nếu hôm nay bạn có kế hoạch ra/vào nhà trong thời gian Alarm hoạt động,
hãy thiết lập "Tạm tắt Alarm hôm nay" để tránh làm phiền các thành viên khác.`,
              true,
              "",
              [
                {
                  homeId,
                  homeName,
                  reasons: [],
                },
              ],
            );
          } else {
            const detail = safety.unsafeDevices.slice(0, 3).join(", ");

            await sendScheduledNotification(
              receiverUid,
              homeId,
              homeName,
              `⚠️ Nhà ${homeName} chưa an toàn: ${detail}

Nếu hôm nay bạn có kế hoạch ra/vào nhà trong thời gian Alarm hoạt động,
hãy thiết lập "Tạm tắt Alarm hôm nay" để tránh làm phiền các thành viên khác.`,
              false,
              detail,
              [
                {
                  homeId,
                  homeName,
                  reasons: safety.unsafeDevices,
                },
              ],
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
async function addHomeEvent(uid, homeId, deviceId, deviceName, text, type = "status") {
  try {
    const now = Date.now();

    const eventRef = db.ref(
      `accounts/${uid}/homes/${homeId}/events/${now}`,
    );

    await eventRef.set({
      time: now,
      deviceId,
      deviceName,
      text,
      type,
    });
    console.log("🏠 HOME EVENT:", homeId, deviceName, text);
    const eventsSnap = await db
      .ref(`accounts/${uid}/homes/${homeId}/events`)
      .once("value");

    const events = eventsSnap.val() || {};
    const keys = Object.keys(events);

    if (keys.length > 200) {
      keys.sort();

      const keysToRemove = keys.slice(0, keys.length - 200);

      for (const key of keysToRemove) {
        await db.ref(`accounts/${uid}/homes/${homeId}/events/${key}`).remove();
      }
    }
  } catch (err) {
    console.log("HOME EVENT ERROR:", err.message);
  }
}
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

    const deviceSnap = await deviceRef.once("value");
    const deviceData = deviceSnap.val() || {};
    const deviceName = deviceData.name || deviceId;

    const notifRef = deviceRef.child(`notifications/${now}`);

    await notifRef.set({
      time: now,
      text,
      type,
    });

    await addHomeEvent(
      uid,
      homeId,
      deviceId,
      deviceName,
      text,
      type,
    );

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
  deviceId,
  deviceName,
  deviceType,
  homeData,
  updateData,
) {
  let deviceAlarm =
    homeData.devices?.[deviceId]?.alarm || null;

  try {
    const modeSnap = await db
      .ref(`accounts/${uid}/customRules/${homeId}/mode`)
      .once("value");

    const mode = modeSnap.val();

    if (mode === "custom") {
      const customAlarmSnap = await db
        .ref(
          `accounts/${uid}/customRules/${homeId}/devices/${deviceId}/alarm`
        )
        .once("value");

      const customAlarm = customAlarmSnap.val();

      if (customAlarm) {
        deviceAlarm = customAlarm;
      }
    }
  } catch (e) {
    console.log("CUSTOM ALARM LOAD ERROR:", e.message);
  }
  const isSecurityDevice =
    deviceType === "door" ||
    deviceType === "door_lock" ||
    deviceType === "motion";

  const isEmergencyDevice =
    deviceType === "smoke" ||
    deviceType === "gas" ||
    deviceType === "water_leak" ||
    deviceType === "sos";
  if (!isSecurityDevice && !isEmergencyDevice) {
    return;
  }
  // các loại khác không kích hoạt alarm
  if (isEmergencyDevice) {
    if (updateData.smoke === true) {
      queueEventAlarm(uid, {
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Phát hiện khói`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });
      return;
    }

    if (updateData.action !== undefined) {
      queueEventAlarm(uid, {
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: SOS được kích hoạt`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });
      return;
    }

    if (updateData.tamper === true) {
      queueEventAlarm(uid, {
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Thiết bị bị tháo`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });
      return;
    }
  }
  if (!isSecurityDevice) {
    return;
  }

  if (!deviceAlarm || deviceAlarm.enabled !== true) {
    return;
  }

  const inTime = isNowInRange(
    deviceAlarm.start,
    deviceAlarm.end,
  );

  if (!inTime) {
    return;
  }

  const oldDevice = homeData.devices?.[deviceId] || {};
  const oldContact = oldDevice.contact;
  const oldTamper = oldDevice.tamper;

  if (updateData.contact === false && oldContact !== false) {
    queueEventAlarm(uid, {
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Cửa mở bất thường`,
      repeatMinutes: 0,
      nextAlarm: getNextAlarmTimeText(deviceAlarm.repeatMinutes),
    });
    return;
  }

  if (updateData.tamper === true && oldTamper !== true) {
    queueEventAlarm(uid, {
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Thiết bị bị tháo`,
      repeatMinutes: 0,
      nextAlarm: getNextAlarmTimeText(deviceAlarm.repeatMinutes),
    });
    return;
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

        const canReceive = await canReceiveAlarm(
          receiverUid,
          homeId,
          ownerUid,
        );
        if (!canReceive) {
          console.log("🔕 ALARM MUTED BY USER:", receiverUid, homeId);
          continue;
        }



        const devices = home.devices || {};

        for (const [deviceId, device] of Object.entries(devices)) {
          const deviceType = device.type || "door";

          const isSecurityDevice =
            deviceType === "door" ||
            deviceType === "door_lock" ||
            deviceType === "motion";

          if (!isSecurityDevice) continue;

          let deviceAlarm = device.alarm;

          try {
            const modeSnap = await db
              .ref(`accounts/${receiverUid}/customRules/${homeId}/mode`)
              .once("value");

            const mode = modeSnap.val();

            if (mode === "custom") {
              const customAlarmSnap = await db
                .ref(
                  `accounts/${receiverUid}/customRules/${homeId}/devices/${deviceId}/alarm`
                )
                .once("value");

              const customAlarm = customAlarmSnap.val();

              if (customAlarm) {
                deviceAlarm = customAlarm;
              }
            }
          } catch (e) {
            console.log("CUSTOM SCHEDULE ALARM ERROR:", e.message);
          }

          if (!deviceAlarm || deviceAlarm.enabled !== true) continue;
          if (!isNowInRange(deviceAlarm.start, deviceAlarm.end)) continue;

          const isUnsafe =
            device.contact === false ||
            device.tamper === true;

          if (!isUnsafe) continue;

          const repeatMinutes = parseInt(deviceAlarm.repeatMinutes || 0);
          const alarmKey = `${receiverUid}_${ownerUid}_${homeId}_${deviceId}_${deviceAlarm.start}_${deviceAlarm.end}_${today}`;
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

          const deviceName = device.name || deviceId;
          const reason = device.contact === false
            ? `${deviceName}: Cửa đang mở`
            : `${deviceName}: Thiết bị bị tháo`;

          if (!alarmSummaryByUser[receiverUid]) {
            alarmSummaryByUser[receiverUid] = [];
          }

          alarmSummaryByUser[receiverUid].push({
            ownerUid,
            homeId,
            homeName: home.name || homeId,
            deviceName,
            type: deviceType,
            reason,
            repeatMinutes,
            nextAlarm: getNextAlarmTimeText(repeatMinutes),
          });
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
  startUserDirectoryListener();
  await db.ref("pair_requests").remove();
  console.log("🧹 OLD PAIR REQUESTS CLEARED");

  setInterval(cleanupExpiredAlarmPause, 60000);
  setInterval(checkScheduledNotifications, 60000);
  setInterval(checkScheduledAlarms, 60000);
}
db.ref("device_delete_requests").on("child_added", async (snap) => {
  try {
    const req = snap.val();

    if (!req) return;
    if (req.status !== "pending") return;

    const { ownerUid, homeId, deviceId } = req;

    console.log("🗑️ DELETE DEVICE:", deviceId);

    client.publish(
      "zigbee2mqtt/bridge/request/device/remove",
      JSON.stringify({
        id: deviceId,
        force: true,
      }),
    );

    await new Promise((r) => setTimeout(r, 3000));

    await db
      .ref(`accounts/${ownerUid}/homes/${homeId}/devices/${deviceId}`)
      .remove();

    await db.ref(`system/devices_by_ieee/${deviceId}`).remove();

    await snap.ref.remove();

    console.log("🧹 DELETE REQUEST REMOVED:", snap.key);

    delete deviceMap[deviceId];

    console.log("✅ DEVICE REMOVED:", deviceId);
  } catch (err) {
    console.log("DELETE DEVICE ERROR:", err.message);
  }
});
db.ref("alarm_pause_requests").on("child_added", async (snap) => {
  try {
    const req = snap.val();

    if (!req) return;
    if (req.status !== "pending") return;

    const {
      ownerUid,
      homeId,
      date,
      start,
      end,
      reason,
      createdByUid,
      createdByName,
      action,
    } = req;

    if (!ownerUid || !homeId) {
      await snap.ref.remove();
      return;
    }

    if (action === "remove") {
      await db
        .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
        .remove();

      const sharedSnap = await db
        .ref(`sharedByHome/${homeId}`)
        .once("value");

      const sharedUsers = sharedSnap.val() || {};

      for (const sharedUid of Object.keys(sharedUsers)) {
        await db
          .ref(
            `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`,
          )
          .remove();
      }

      await snap.ref.remove();

      console.log(
        "🧹 ALARM PAUSE REMOVED:",
        ownerUid,
        homeId,
      );

      return;
    }

    if (!date || !start || !end) {
      await snap.ref.remove();
      return;
    }

    const pauseData = {
      date,
      start,
      end,
      reason: reason || "",
      createdByUid: createdByUid || "",
      createdByName: createdByName || "",
      createdAt: Date.now(),
    };

    await db
      .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
      .set(pauseData);
    console.log(
      "PAUSE DEBUG:",
      ownerUid,
      createdByUid,
      createdByName,
    );
    const sharedSnap = await db
      .ref(`sharedByHome/${homeId}`)
      .once("value");

    const sharedUsers = sharedSnap.val() || {};

    for (const sharedUid of Object.keys(sharedUsers)) {
      await db
        .ref(
          `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`,
        )
        .set(pauseData);
    }

    await snap.ref.remove();

    console.log("⏸️ ALARM PAUSE REQUEST APPLIED:", ownerUid, homeId);
  } catch (err) {
    console.log("ALARM PAUSE REQUEST ERROR:", err.message);
  }
});
db.ref("accounts").on("child_changed", async (snap) => {
  try {
    const ownerUid = snap.key;
    const user = snap.val() || {};
    const homes = user.homes || {};

    for (const [homeId, home] of Object.entries(homes)) {
      const pause = home.alarmPauseToday;

      if (!pause) continue;

      const key =
        `${ownerUid}_${homeId}_${pause.createdAt || 0}`;

      if (lastNotificationMap[key]) {
        continue;
      }

      lastNotificationMap[key] = Date.now();

      const homeName =
        (pause.homeName && pause.homeName.trim()) ||
        (home.name && home.name.trim()) ||
        homeId;

      const actorName =
        pause.createdByName &&
          pause.createdByName.trim().length > 0
          ? pause.createdByName.trim()
          : "Một thành viên";

      const text =
        `Alarm đã được ${actorName} tạm tắt từ ${pause.start} tới ${pause.end}.

Nên trong khoảng thời gian này:
• Một số thiết bị an ninh sẽ tạm ngừng cảnh báo.
• Các cảnh báo nguy hiểm như cháy nổ, ngập nước, chạm chập v.v... vẫn được gửi bình thường.`;

      const sharedSnap = await db
        .ref(`sharedByHome/${homeId}`)
        .once("value");

      const sharedUsers = sharedSnap.val() || {};

      const recipientUids = new Set([
        ownerUid,
        ...Object.keys(sharedUsers),
      ]);

      const pauseReason =
        String(pause.reason || "").trim();

      const homeNotificationMessage =
        `${actorName} đã tạm dừng Alarm từ ${pause.start} đến ${pause.end}.` +
        (
          pauseReason.length > 0
            ? ` Lý do: ${pauseReason}.`
            : ""
        );

      for (const recipientUid of recipientUids) {
        await addHomeNotificationFromBackend({
          uid: recipientUid,
          homeId,
          homeName,
          type: "alarm_pause_started",
          category: "alarm",
          severity: "warning",
          title: "Alarm đã được tạm dừng",
          message: homeNotificationMessage,
          entityType: "home",
          entityId: homeId,
        });

        if (recipientUid === pause.createdByUid) {
          continue;
        }

        await sendAlarmPauseNotification(
          recipientUid,
          homeId,
          homeName,
          text,
        );
      }

      console.log(
        "⏸️ ALARM PAUSE BROADCAST:",
        homeId,
      );
    }
  } catch (err) {
    console.log(
      "ALARM PAUSE WATCH ERROR:",
      err.message,
    );
  }
});
init();

// ================= MQTT CONNECT =================
client.on("connect", () => {
  console.log("MQTT CONNECTED");
  client.subscribe("zigbee2mqtt/#");
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
    uid: data.ownerUid || data.requestedBy,
    requestedBy: data.requestedBy,
    homeId: data.homeId,
    roomId: data.roomId || "unassigned",
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

        const { uid, homeId, roomId } = pairingSession;

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
          roomId: roomId || "unassigned",
          alarm:
            deviceType === "door"
              ? {
                enabled: true,
                start: "23:00",
                end: "06:00",
                repeatMinutes: 30,
              }
              : null,
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

      let map = deviceMap[deviceId];

      if (!map) {
        const snap = await db
          .ref(`system/devices_by_ieee/${deviceId}`)
          .once("value");

        const found = snap.val();

        if (!found?.uid || !found?.homeId) {
          console.log("⚠️ AVAILABILITY NO MAP:", deviceId);
          return;
        }

        map = {
          uid: found.uid,
          homeId: found.homeId,
        };

        deviceMap[deviceId] = map;
      }

      const { uid, homeId } = map;

      const deviceRef = db.ref(
        `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
      );

      const deviceSnap = await deviceRef.once("value");

      if (!deviceSnap.exists()) {
        console.log(
          "🧹 DEVICE DELETED FROM APP, REMOVE MAP:",
          deviceId,
        );

        delete deviceMap[deviceId];

        await db.ref(`system/devices_by_ieee/${deviceId}`).remove();

        return;
      }

      await deviceRef.update({
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
      deviceId,
      deviceName,
      updateData.type || oldData.type || "door",
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
        deviceId,
        deviceName,
        updateData.type || oldData.type || "door",
        homeData,
        updateData,
      );
    }
  } catch (err) {
    console.log("MQTT ERROR:", err.message);
  }
});