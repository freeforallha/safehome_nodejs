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
const processedChatNotificationMessages = new Set();

// ================= ALARM ESCALATION =================
// Sự cố an ninh thông thường:
// Phát hiện → Alarm sau 15 giây → Còi lớn sau 30 giây
// → chuẩn bị gọi điện sau 60 giây.
const ALARM_INCIDENT_ALARM_DELAY_MS = 15 * 1000;
const ALARM_INCIDENT_SIREN_DELAY_MS = 30 * 1000;
const ALARM_INCIDENT_CALL_DELAY_MS = 60 * 1000;

// Sự cố khẩn cấp: SOS, khói/cháy, gas, ngập nước.
// Notification ưu tiên ngay → Fullscreen + điểm nối còi trong nhà sau 5 giây
// → chuẩn bị gọi điện sau tổng cộng 35 giây.
const EMERGENCY_FULLSCREEN_DELAY_MS = 5 * 1000;
const EMERGENCY_CALL_DELAY_MS = 35 * 1000;
// Chỉ gộp các packet liên tiếp của cùng một lần kích hoạt.
// Sau khoảng này, lần mở cửa / SOS mới phải tạo incident mới
// để notification và các cấp sau chạy lại từ đầu.
const SECURITY_MERGE_WINDOW_MS = 10 * 1000;
const EMERGENCY_MERGE_WINDOW_MS = 10 * 1000;

const ALARM_INCIDENT_AUTO_EXPIRE_MS = 30 * 60 * 1000;

// Hub ghi heartbeat lên Firebase mỗi 30 giây.
// App sẽ coi hub Offline khi lastHeartbeatAt quá 90 giây.
const HUB_HEARTBEAT_INTERVAL_MS = 30 * 1000;
const HUB_HEARTBEAT_STARTED_AT = Date.now();

const alarmIncidentTimerMap = {};
const alarmIncidentAdvanceInProgress = new Set();
const alarmIncidentActionInProgress = new Set();
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

let hubHeartbeatTimer = null;
let hubHeartbeatWriteInProgress = false;
let hubHeartbeatWriteCount = 0;
let mqttConnected = false;

async function getHomesLinkedToThisHub() {
  const snap = await db
    .ref("system/devices_by_ieee")
    .once("value");

  const index = snap.val() || {};
  const homes = new Map();

  for (const value of Object.values(index)) {
    const item = value || {};
    const itemHubId = String(
      item.deviceId || "",
    ).trim();

    if (itemHubId !== DEVICE_ID) {
      continue;
    }

    const uid = String(item.uid || "").trim();
    const homeId = String(item.homeId || "").trim();

    if (!uid || !homeId) {
      continue;
    }

    homes.set(
      `${uid}|${homeId}`,
      {
        uid,
        homeId,
      },
    );
  }

  return [...homes.values()];
}

async function writeHubHeartbeat() {
  if (hubHeartbeatWriteInProgress) {
    return;
  }

  hubHeartbeatWriteInProgress = true;

  try {
    const linkedHomes =
      await getHomesLinkedToThisHub();

    const now = Date.now();

    const heartbeat = {
      hubId: DEVICE_ID,
      hubType: "raspberry_pi",
      status: "online",
      mqttConnected,
      backendPid: process.pid,
      startedAt: HUB_HEARTBEAT_STARTED_AT,
      lastHeartbeatAt: now,
      heartbeatIntervalMs:
        HUB_HEARTBEAT_INTERVAL_MS,
    };

    const updates = {
      [`system/hubs/${DEVICE_ID}`]: heartbeat,
    };

    for (const item of linkedHomes) {
      const basePath =
        `accounts/${item.uid}/homes/${item.homeId}`;

      // Lưu liên kết Hub trực tiếp trong nhà để app không phải
      // đọc toàn bộ system/devices_by_ieee.
      updates[`${basePath}/hubId`] = DEVICE_ID;
      updates[`${basePath}/hubStatus`] = heartbeat;
    }

    await db.ref().update(updates);

    hubHeartbeatWriteCount++;

    // Chỉ ghi log lần đầu và mỗi 10 phút để tránh làm đầy journal.
    if (
      hubHeartbeatWriteCount === 1 ||
      hubHeartbeatWriteCount % 20 === 0
    ) {
      console.log(
        "💓 HUB HEARTBEAT:",
        DEVICE_ID,
        `homes=${linkedHomes.length}`,
        `mqtt=${mqttConnected}`,
      );
    }
  } catch (err) {
    console.log(
      "HUB HEARTBEAT ERROR:",
      err.message,
    );
  } finally {
    hubHeartbeatWriteInProgress = false;
  }
}

function startHubHeartbeat() {
  if (hubHeartbeatTimer) {
    return;
  }

  void writeHubHeartbeat();

  hubHeartbeatTimer = setInterval(
    () => {
      void writeHubHeartbeat();
    },
    HUB_HEARTBEAT_INTERVAL_MS,
  );

  console.log(
    "💓 HUB HEARTBEAT STARTED:",
    DEVICE_ID,
    `interval=${HUB_HEARTBEAT_INTERVAL_MS / 1000}s`,
  );
}

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
    return nowMin >= start || nowMin < end;
  }

  return nowMin >= start && nowMin < end;
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
function includesAny(text, keywords) {
  return keywords.some((keyword) => {
    return text.includes(keyword);
  });
}

function isActiveSignal(value) {
  if (value === true || value === 1) {
    return true;
  }

  const normalized = String(value || "")
    .trim()
    .toLowerCase();

  return (
    normalized === "true" ||
    normalized === "1" ||
    normalized === "on" ||
    normalized === "active" ||
    normalized === "alarm" ||
    normalized === "detected" ||
    normalized === "triggered" ||
    normalized === "emergency" ||
    normalized === "unsafe" ||
    normalized === "open" ||
    normalized === "unlocked"
  );
}

function normalizeLockState(device) {
  const raw =
    device?.lock_state ??
    device?.lockState ??
    device?.lock ??
    device?.state;

  if (raw === true || raw === 1) {
    return "locked";
  }

  if (raw === false || raw === 0) {
    return "unlocked";
  }

  const normalized = String(raw || "")
    .trim()
    .toLowerCase();

  if (
    normalized === "lock" ||
    normalized === "locked" ||
    normalized === "closed"
  ) {
    return "locked";
  }

  if (
    normalized === "unlock" ||
    normalized === "unlocked" ||
    normalized === "open"
  ) {
    return "unlocked";
  }

  return "";
}

function inferDeviceTypeFromPayload(
  data,
  currentType = "unknown",
) {
  const normalizedCurrentType = String(
    currentType || "unknown",
  ).trim();

  if (
    normalizedCurrentType &&
    normalizedCurrentType !== "unknown"
  ) {
    return normalizedCurrentType;
  }

  if (data.smoke !== undefined) {
    return "smoke";
  }

  if (
    data.carbon_monoxide !== undefined ||
    data.co_alarm !== undefined
  ) {
    return "carbon_monoxide";
  }

  if (
    data.gas !== undefined ||
    data.gas_alarm !== undefined
  ) {
    return "gas";
  }

  if (
    data.water_leak !== undefined ||
    data.leak !== undefined ||
    data.water !== undefined
  ) {
    return "water_leak";
  }

  if (
    data.heat !== undefined ||
    data.heat_alarm !== undefined ||
    data.high_temperature_alarm !== undefined
  ) {
    return "heat";
  }

  if (
    data.occupancy !== undefined ||
    data.motion !== undefined
  ) {
    return "motion";
  }

  if (data.presence !== undefined) {
    return "presence";
  }

  if (
    data.vibration !== undefined ||
    data.vibration_strength !== undefined
  ) {
    return "vibration";
  }

  if (data.contact !== undefined) {
    return "door";
  }

  if (
    data.power !== undefined ||
    data.current !== undefined ||
    data.voltage !== undefined ||
    data.energy !== undefined
  ) {
    return "smart_plug";
  }

  if (
    data.temperature !== undefined ||
    data.humidity !== undefined
  ) {
    return "temperature";
  }

  return "unknown";
}

function getDeviceTypeFromModel(modelId, description, ieee) {
  const id = String(ieee || "").trim().toLowerCase();
  const model = String(modelId || "").trim().toLowerCase();
  const desc = String(description || "").trim().toLowerCase();
  const searchable = `${model} ${desc}`;

  // Các thiết bị đang dùng thực tế.
  if (id === "0xa4c1388295d25926") return "smoke";
  if (id === "0xa4c138b872c891a2") return "temperature";
  if (id === "0xa4c1381162d4d15b") return "sos";
  if (id === "0xa4c13898b084dbdc") return "repeater";

  // Loại cụ thể phải được kiểm tra trước loại chung.
  if (
    includesAny(searchable, [
      "smart lock",
      "door lock",
      "deadbolt",
      "keyless lock",
      "electronic lock",
    ])
  ) {
    return "door_lock";
  }

  if (
    includesAny(searchable, [
      "glass break",
      "glassbreak",
      "broken glass",
    ])
  ) {
    return "glass_break";
  }

  if (
    includesAny(searchable, [
      "carbon monoxide",
      "carbon-monoxide",
      "co alarm",
      "co sensor",
    ]) &&
    !searchable.includes("co2")
  ) {
    return "carbon_monoxide";
  }

  if (
    includesAny(searchable, [
      "water leak",
      "water leakage",
      "leak sensor",
      "flood sensor",
      "water sensor",
    ])
  ) {
    return "water_leak";
  }

  if (
    includesAny(searchable, [
      "combustible gas",
      "natural gas",
      "gas detector",
      "gas sensor",
      "methane",
      "lpg",
    ])
  ) {
    return "gas";
  }

  if (
    includesAny(searchable, [
      "heat detector",
      "heat alarm",
      "temperature alarm",
    ])
  ) {
    return "heat";
  }

  if (
    includesAny(searchable, [
      "smoke",
      "fire detector",
      "fire alarm",
    ]) ||
    model.includes("ts0205")
  ) {
    return "smoke";
  }

  if (
    includesAny(searchable, [
      "presence sensor",
      "human presence",
      "mmwave",
      "radar presence",
    ])
  ) {
    return "presence";
  }

  if (
    includesAny(searchable, [
      "motion sensor",
      "pir sensor",
      "occupancy sensor",
      "motion detector",
    ]) ||
    model.includes("snzb-03") ||
    model.includes("ts0202")
  ) {
    return "motion";
  }

  if (
    includesAny(searchable, [
      "vibration",
      "shock sensor",
      "tilt sensor",
    ]) ||
    model.includes("djt11lm")
  ) {
    return "vibration";
  }

  if (
    includesAny(searchable, [
      "door contact",
      "window contact",
      "contact sensor",
      "door sensor",
      "window sensor",
      "open close sensor",
    ]) ||
    model.includes("snzb-04") ||
    model.includes("ts0203")
  ) {
    if (desc.includes("window")) return "window";
    return "door";
  }

  if (
    includesAny(searchable, [
      "panic button",
      "sos",
      "emergency button",
    ])
  ) {
    return "sos";
  }

  if (
    includesAny(searchable, [
      "smart plug",
      "smart socket",
      "wall plug",
      "power outlet",
      "socket outlet",
    ]) ||
    model.includes("ts011f")
  ) {
    return "smart_plug";
  }

  if (
    includesAny(searchable, [
      "power monitor",
      "energy monitor",
      "clamp meter",
    ])
  ) {
    return "power_monitor";
  }

  if (includesAny(searchable, ["ups", "backup power"])) {
    return "ups";
  }

  if (
    includesAny(searchable, [
      "siren",
      "alarm bell",
      "warning horn",
    ])
  ) {
    return "siren";
  }

  if (
    includesAny(searchable, [
      "water valve",
      "gas valve",
      "valve controller",
      "smart valve",
    ])
  ) {
    return "smart_valve";
  }

  if (includesAny(searchable, ["doorbell", "video bell"])) {
    return "doorbell";
  }

  if (includesAny(searchable, ["camera", "ip cam"])) {
    return "camera";
  }

  if (includesAny(searchable, ["keypad", "key fob"])) {
    return "keypad";
  }

  if (
    includesAny(searchable, [
      "temperature",
      "humidity",
      "thermometer",
      "hygrometer",
    ])
  ) {
    return "temperature";
  }

  if (
    includesAny(searchable, [
      "repeater",
      "range extender",
      "signal extender",
    ])
  ) {
    return "repeater";
  }

  // Không mặc định thành cửa để tránh một thiết bị lạ
  // vô tình tạo Alarm sai.
  return "unknown";
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

function normalizeFcmToken(raw) {
  return String(raw || "").trim();
}

async function getUserFcmTargets(uid) {
  const accountSnap = await db
    .ref(`accounts/${uid}`)
    .once("value");

  const account = accountSnap.val() || {};
  const targetsByToken = new Map();

  function addTarget(tokenValue, path) {
    const token = normalizeFcmToken(tokenValue);

    if (!token || !path) {
      return;
    }

    if (!targetsByToken.has(token)) {
      targetsByToken.set(token, {
        token,
        paths: new Set(),
      });
    }

    targetsByToken.get(token).paths.add(path);
  }

  addTarget(
    account.fcmToken,
    `accounts/${uid}/fcmToken`,
  );

  const installations =
    account.fcmTokens &&
    typeof account.fcmTokens === "object"
      ? account.fcmTokens
      : {};

  for (const [installationId, rawEntry] of Object.entries(
    installations,
  )) {
    const entryPath =
      `accounts/${uid}/fcmTokens/${installationId}`;

    if (typeof rawEntry === "string") {
      addTarget(rawEntry, entryPath);
      continue;
    }

    if (rawEntry && typeof rawEntry === "object") {
      addTarget(rawEntry.token, entryPath);
    }
  }

  return Array.from(targetsByToken.values()).map(
    (target) => ({
      token: target.token,
      paths: Array.from(target.paths),
    }),
  );
}

function isInvalidFcmTokenError(error) {
  const code = String(
    error?.errorInfo?.code ||
    error?.code ||
    "",
  );

  return (
    code ===
      "messaging/registration-token-not-registered" ||
    code ===
      "messaging/invalid-registration-token"
  );
}

async function removeInvalidFcmTokenPaths(paths) {
  const updates = {};

  for (const path of paths) {
    if (path) {
      updates[path] = null;
    }
  }

  if (Object.keys(updates).length === 0) {
    return;
  }

  await db.ref().update(updates);
}

async function sendPushToUser(
  uid,
  message,
  logLabel = "PUSH",
) {
  const targets = await getUserFcmTargets(uid);

  if (targets.length === 0) {
    console.log(
      "❌ NO FCM TOKEN:",
      uid,
      logLabel,
    );

    return {
      total: 0,
      sent: 0,
      failed: 0,
      removed: 0,
    };
  }

  let sent = 0;
  let failed = 0;
  const invalidPaths = new Set();

  for (const target of targets) {
    try {
      await admin.messaging().send({
        ...message,
        token: target.token,
      });

      sent++;
    } catch (error) {
      failed++;

      if (isInvalidFcmTokenError(error)) {
        for (const path of target.paths) {
          invalidPaths.add(path);
        }

        console.log(
          "🧹 INVALID FCM TOKEN:",
          uid,
          logLabel,
          error?.errorInfo?.code ||
            error?.code ||
            error.message,
        );

        continue;
      }

      console.log(
        "FCM TARGET SEND ERROR:",
        uid,
        logLabel,
        error.message,
      );
    }
  }

  if (invalidPaths.size > 0) {
    try {
      await removeInvalidFcmTokenPaths(
        Array.from(invalidPaths),
      );
    } catch (error) {
      console.log(
        "FCM TOKEN CLEANUP ERROR:",
        uid,
        logLabel,
        error.message,
      );
    }
  }

  console.log(
    "📨 MULTI-DEVICE PUSH:",
    uid,
    logLabel,
    `sent=${sent}`,
    `failed=${failed}`,
    `targets=${targets.length}`,
  );

  return {
    total: targets.length,
    sent,
    failed,
    removed: invalidPaths.size,
  };
}

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

    const pushResult = await sendPushToUser(
      uid,
      {
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
      },
      "SCHEDULE SUMMARY",
    );

    if (pushResult.sent === 0) {
      return;
    }

    console.log(
      "🔔 SCHEDULE SUMMARY SENT:",
      uid,
      uniqueItems.length,
      `devices=${pushResult.sent}`,
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
    const pushResult = await sendPushToUser(
      uid,
      {
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
      },
      "ALARM",
    );

    if (pushResult.sent === 0) {
      return;
    }

    console.log(
      "🚨 PUSH SENT:",
      uid,
      homeId,
      `devices=${pushResult.sent}`,
    );
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
    const pushResult = await sendPushToUser(
      uid,
      {
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

        // Data-only để app dùng đúng một notification Reminder
        // ID 999998 và channel ưu tiên mới.
        android: {
          priority: "high",
        },
      },
      "ALARM PAUSE",
    );

    if (pushResult.sent === 0) {
      return;
    }

    console.log(
      "⏸️ ALARM PAUSE WARNING SENT:",
      uid,
      homeId,
      `devices=${pushResult.sent}`,
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
function getAlarmIncidentTargetKey(
  receiverUid,
  ownerUid,
  homeId,
  flowType = "security",
) {
  return crypto
    .createHash("sha256")
    .update(
      [
        String(receiverUid || ""),
        String(ownerUid || ""),
        String(homeId || ""),
        String(flowType || "security"),
      ].join("|"),
    )
    .digest("hex")
    .slice(0, 24);
}

function getAlarmIncidentTimerKey(uid, incidentId) {
  return `${uid}_${incidentId}`;
}

function clearAlarmIncidentTimers(uid, incidentId) {
  const key = getAlarmIncidentTimerKey(uid, incidentId);
  const timers = alarmIncidentTimerMap[key] || {};

  for (const timer of Object.values(timers)) {
    if (timer) {
      clearTimeout(timer);
    }
  }

  delete alarmIncidentTimerMap[key];
}

function normalizeAlarmIncidentItems(items) {
  const uniqueItems = [];

  for (const rawItem of Array.isArray(items) ? items : []) {
    const item = rawItem || {};
    const homeId = String(item.homeId || "").trim();
    const reason = String(item.reason || "").trim();

    if (!homeId || !reason) {
      continue;
    }

    const exists = uniqueItems.some((oldItem) => {
      return (
        oldItem.homeId === homeId &&
        oldItem.reason === reason
      );
    });

    if (!exists) {
      uniqueItems.push({
        ownerUid: String(item.ownerUid || "").trim(),
        homeId,
        homeName:
          String(item.homeName || "").trim() || homeId,
        deviceName: String(item.deviceName || "").trim(),
        type: String(item.type || "").trim(),
        reason,
        repeatMinutes: normalizeRepeatMinutes(
          item.repeatMinutes,
        ),
        nextAlarm: String(item.nextAlarm || "").trim(),
      });
    }
  }

  return uniqueItems.slice(0, 20);
}

function getAlarmIncidentFlowType(items) {
  const normalized = normalizeAlarmIncidentItems(items);

  return normalized.some((item) => {
    return isEmergencyDeviceType(
      String(item.type || "").trim(),
    );
  })
    ? "emergency"
    : "security";
}

function getEmergencyIncidentTitle(items) {
  const types = new Set(
    normalizeAlarmIncidentItems(items).map((item) => {
      return String(item.type || "").trim();
    }),
  );

  if (types.has("sos")) {
    return "🆘 SOS KHẨN CẤP";
  }

  if (types.has("smoke")) {
    return "🔥 CẢNH BÁO KHÓI / CHÁY";
  }

  if (types.has("heat")) {
    return "🌡️ CẢNH BÁO NHIỆT ĐỘ NGUY HIỂM";
  }

  if (types.has("carbon_monoxide")) {
    return "☠️ CẢNH BÁO KHÍ CO";
  }

  if (types.has("gas")) {
    return "⚠️ CẢNH BÁO RÒ RỈ GAS";
  }

  if (
    types.has("water_leak") ||
    types.has("flood")
  ) {
    return "🌊 CẢNH BÁO NGẬP NƯỚC";
  }

  return "🚨 CẢNH BÁO KHẨN CẤP";
}


function getAlarmIncidentLines(items) {
  const normalized = normalizeAlarmIncidentItems(items);
  const lines = normalized.slice(0, 4).map((item) => {
    return `${item.homeName}: ${item.reason}`;
  });

  if (normalized.length > 4) {
    lines.push("...");
  }

  return lines;
}

async function sendAlarmStageSummary(
  uid,
  items,
  {
    incidentId = "",
    stage = "alarm",
    flowType = "security",
  } = {},
) {
  try {
    const uniqueItems = normalizeAlarmIncidentItems(items);

    if (uniqueItems.length === 0) {
      return false;
    }

    const isEmergency = flowType === "emergency";
    const allowedItems = [];

    for (const item of uniqueItems) {
      // Emergency không bị chặn bởi lịch, Mode,
      // tạm dừng Alarm hoặc cài đặt nhận Alarm.
      if (isEmergency) {
        allowedItems.push(item);
        continue;
      }

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
      console.log(
        "🔕 ALARM INCIDENT MUTED:",
        uid,
        incidentId,
        stage,
      );
      return false;
    }

    const lines = getAlarmIncidentLines(allowedItems);
    const body = lines.join("\n");

    let type = "alarm";
    let title = "🚨 SAFEHOME";
    let clickAction = "alarm_SCREEN";
    let apnsSound = "default";

    if (
      isEmergency &&
      stage === "notification"
    ) {
      type = "emergency_notification";
      title = getEmergencyIncidentTitle(allowedItems);
      clickAction = "emergency_NOTIFICATION";
      apnsSound = "default";
    } else if (
      isEmergency &&
      stage === "fullscreen_siren"
    ) {
      type = "alarm_siren";
      title = getEmergencyIncidentTitle(allowedItems);
      clickAction = "alarm_SIREN_SCREEN";
      apnsSound = "default";
    } else if (stage === "detected") {
      type = "alarm_detected";
      title = "SafeHome phát hiện bất thường";
      clickAction = "alarm_detected";
      apnsSound = null;
    } else if (stage === "siren") {
      type = "alarm_siren";
      title = "🚨 CẢNH BÁO KHẨN CẤP";
      clickAction = "alarm_SIREN_SCREEN";
      apnsSound = "default";
    }

    // Incident được nhóm theo từng nhà, nên dù có nhiều sensor
    // thì homeId/ownerUid vẫn phải luôn có để app xác nhận đúng sự cố.
    const incidentHomeId = String(
      allowedItems[0]?.homeId || "",
    );

    const incidentOwnerUid = String(
      allowedItems[0]?.ownerUid || "",
    );

    const message = {
      data: {
        type,
        title,
        body,
        alarmItems: JSON.stringify(allowedItems),
        incidentId: String(incidentId || ""),
        alarmStage: stage,
        alarmFlowType: flowType,
        homeId: incidentHomeId,
        ownerUid: incidentOwnerUid,
        clickAction,
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
              title,
              body,
            },
            badge: 1,
          },
        },
      },
    };

    if (apnsSound) {
      message.apns.payload.aps.sound = apnsSound;
    }

    const pushResult = await sendPushToUser(
      uid,
      message,
      `ALARM INCIDENT ${flowType}/${stage}`,
    );

    if (pushResult.sent === 0) {
      return false;
    }

    console.log(
      "🚨 ALARM INCIDENT PUSH:",
      uid,
      incidentId,
      flowType,
      stage,
      allowedItems.length,
      `devices=${pushResult.sent}`,
    );

    return true;
  } catch (err) {
    console.log(
      "ALARM INCIDENT PUSH ERROR:",
      uid,
      incidentId,
      flowType,
      stage,
      err.message,
    );
    return false;
  }
}

async function sendAlarmResolvedPush({
  uid,
  incidentId,
  homeId,
  resolvedBy,
  action,
}) {
  try {
    await sendPushToUser(
      uid,
      {
        data: {
          type: "alarm_resolved",
          incidentId: String(incidentId || ""),
          homeId: String(homeId || ""),
          resolvedBy: String(resolvedBy || ""),
          action: String(action || "resolved"),
          clickAction: "alarm_RESOLVED",
        },
        android: {
          priority: "high",
        },
      },
      "ALARM RESOLVED",
    );
  } catch (err) {
    console.log(
      "ALARM RESOLVED PUSH ERROR:",
      uid,
      incidentId,
      err.message,
    );
  }
}

async function getActiveAlarmIncident(
  receiverUid,
  targetKey,
) {
  const indexRef = db.ref(
    `accounts/${receiverUid}/activeAlarmIncidentByTarget/${targetKey}`,
  );

  const incidentIdSnap = await indexRef.once("value");
  const incidentId = String(
    incidentIdSnap.val() || "",
  ).trim();

  if (!incidentId) {
    return null;
  }

  const incidentRef = db.ref(
    `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
  );

  const incidentSnap = await incidentRef.once("value");
  const incident = incidentSnap.val();

  if (!incident || incident.status !== "active") {
    await indexRef.remove();
    return null;
  }

  return {
    incidentId,
    incident,
  };
}

async function advanceAlarmIncidentToStage(
  receiverUid,
  incidentId,
  targetStage,
) {
  const lockKey = `${receiverUid}_${incidentId}`;

  if (alarmIncidentAdvanceInProgress.has(lockKey)) {
    return;
  }

  alarmIncidentAdvanceInProgress.add(lockKey);

  try {
    const incidentRef = db.ref(
      `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
    );

    let incidentSnap = await incidentRef.once("value");
    let incident = incidentSnap.val();

    if (!incident || incident.status !== "active") {
      return;
    }

    const flowType =
      incident.flowType === "emergency"
        ? "emergency"
        : "security";

    const order = flowType === "emergency"
      ? [
          "notification",
          "fullscreen_siren",
          "calling",
        ]
      : [
          "detected",
          "alarm",
          "siren",
          "calling",
        ];

    const targetRank = order.indexOf(targetStage);

    if (targetRank < 1) {
      return;
    }

    let currentRank = order.indexOf(
      String(
        incident.stage ||
        (flowType === "emergency"
          ? "notification"
          : "detected"),
      ),
    );

    if (currentRank < 0) {
      currentRank = 0;
    }

    for (
      let nextRank = currentRank + 1;
      nextRank <= targetRank;
      nextRank++
    ) {
      incidentSnap = await incidentRef.once("value");
      incident = incidentSnap.val();

      if (!incident || incident.status !== "active") {
        return;
      }

      const nextStage = order[nextRank];
      const items = normalizeAlarmIncidentItems(
        incident.items,
      );
      const now = Date.now();

      if (nextStage === "alarm") {
        await sendAlarmStageSummary(
          receiverUid,
          items,
          {
            incidentId,
            stage: "alarm",
            flowType,
          },
        );

        await incidentRef.update({
          stage: "alarm",
          alarmSentAt: now,
          updatedAt: now,
        });
      } else if (nextStage === "siren") {
        await sendAlarmStageSummary(
          receiverUid,
          items,
          {
            incidentId,
            stage: "siren",
            flowType,
          },
        );

        await incidentRef.update({
          stage: "siren",
          sirenSentAt: now,
          updatedAt: now,
        });
      } else if (
        nextStage === "fullscreen_siren"
      ) {
        await sendAlarmStageSummary(
          receiverUid,
          items,
          {
            incidentId,
            stage: "fullscreen_siren",
            flowType,
          },
        );

        await incidentRef.update({
          stage: "fullscreen_siren",
          fullscreenSentAt: now,
          // Điểm nối cho còi vật lý trong nhà.
          homeSirenStatus: "waiting_devices",
          homeSirenRequestedAt: now,
          updatedAt: now,
        });

        console.log(
          "📢 HOME SIREN STAGE READY:",
          receiverUid,
          incidentId,
          incident.homeId,
        );
      } else if (nextStage === "calling") {
        // Chưa gọi thật cho tới khi kết nối Cloud Telephony.
        await incidentRef.update({
          stage: "calling",
          callStatus: "waiting_provider",
          callRequestedAt: now,
          updatedAt: now,
        });

        console.log(
          "📞 ALARM CALL STAGE READY:",
          receiverUid,
          incidentId,
          incident.homeId,
        );
      }
    }
  } catch (err) {
    console.log(
      "ALARM INCIDENT ADVANCE ERROR:",
      receiverUid,
      incidentId,
      targetStage,
      err.message,
    );
  } finally {
    alarmIncidentAdvanceInProgress.delete(lockKey);
  }
}

async function expireAlarmIncident(
  receiverUid,
  incidentId,
) {
  try {
    const incidentRef = db.ref(
      `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
    );

    const snap = await incidentRef.once("value");
    const incident = snap.val();

    if (!incident || incident.status !== "active") {
      clearAlarmIncidentTimers(receiverUid, incidentId);
      return;
    }

    const targetKey = String(
      incident.targetKey || "",
    ).trim();

    const updates = {
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/status`]:
        "expired",
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/stage`]:
        "expired",
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/expiredAt`]:
        Date.now(),
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/updatedAt`]:
        Date.now(),
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/homeSirenStatus`]:
        "stop_requested",
    };

    if (targetKey) {
      updates[
        `accounts/${receiverUid}/activeAlarmIncidentByTarget/${targetKey}`
      ] = null;
    }

    await db.ref().update(updates);
    clearAlarmIncidentTimers(receiverUid, incidentId);

    console.log(
      "⌛ ALARM INCIDENT EXPIRED:",
      receiverUid,
      incidentId,
    );
  } catch (err) {
    console.log(
      "ALARM INCIDENT EXPIRE ERROR:",
      receiverUid,
      incidentId,
      err.message,
    );
  }
}

function scheduleAlarmIncidentStages(
  receiverUid,
  incidentId,
  incident,
) {
  clearAlarmIncidentTimers(receiverUid, incidentId);

  if (!incident || incident.status !== "active") {
    return;
  }

  const key = getAlarmIncidentTimerKey(
    receiverUid,
    incidentId,
  );

  const now = Date.now();
  const detectedAt = Number(
    incident.detectedAt || incident.createdAt || now,
  );

  const expireAt = Number(
    incident.expireAt ||
      detectedAt + ALARM_INCIDENT_AUTO_EXPIRE_MS,
  );

  const flowType =
    incident.flowType === "emergency"
      ? "emergency"
      : "security";

  if (flowType === "emergency") {
    const fullscreenDueAt = Number(
      incident.fullscreenDueAt ||
        detectedAt + EMERGENCY_FULLSCREEN_DELAY_MS,
    );

    const callDueAt = Number(
      incident.callDueAt ||
        detectedAt + EMERGENCY_CALL_DELAY_MS,
    );

    alarmIncidentTimerMap[key] = {
      fullscreenSiren: setTimeout(
        () => {
          void advanceAlarmIncidentToStage(
            receiverUid,
            incidentId,
            "fullscreen_siren",
          );
        },
        Math.max(0, fullscreenDueAt - now),
      ),
      calling: setTimeout(
        () => {
          void advanceAlarmIncidentToStage(
            receiverUid,
            incidentId,
            "calling",
          );
        },
        Math.max(0, callDueAt - now),
      ),
      expire: setTimeout(
        () => {
          void expireAlarmIncident(
            receiverUid,
            incidentId,
          );
        },
        Math.max(0, expireAt - now),
      ),
    };

    return;
  }

  const alarmDueAt = Number(
    incident.alarmDueAt ||
      detectedAt + ALARM_INCIDENT_ALARM_DELAY_MS,
  );

  const sirenDueAt = Number(
    incident.sirenDueAt ||
      detectedAt + ALARM_INCIDENT_SIREN_DELAY_MS,
  );

  const callDueAt = Number(
    incident.callDueAt ||
      detectedAt + ALARM_INCIDENT_CALL_DELAY_MS,
  );

  alarmIncidentTimerMap[key] = {
    alarm: setTimeout(
      () => {
        void advanceAlarmIncidentToStage(
          receiverUid,
          incidentId,
          "alarm",
        );
      },
      Math.max(0, alarmDueAt - now),
    ),
    siren: setTimeout(
      () => {
        void advanceAlarmIncidentToStage(
          receiverUid,
          incidentId,
          "siren",
        );
      },
      Math.max(0, sirenDueAt - now),
    ),
    calling: setTimeout(
      () => {
        void advanceAlarmIncidentToStage(
          receiverUid,
          incidentId,
          "calling",
        );
      },
      Math.max(0, callDueAt - now),
    ),
    expire: setTimeout(
      () => {
        void expireAlarmIncident(
          receiverUid,
          incidentId,
        );
      },
      Math.max(0, expireAt - now),
    ),
  };
}

async function startOrMergeAlarmIncidents(uid, items) {
  const normalizedItems = normalizeAlarmIncidentItems(items);

  if (normalizedItems.length === 0) {
    return;
  }

  const groups = new Map();

  for (const item of normalizedItems) {
    const ownerUid = item.ownerUid || uid;
    const flowType = getAlarmIncidentFlowType([item]);
    const groupKey =
      `${ownerUid}|${item.homeId}|${flowType}`;

    if (!groups.has(groupKey)) {
      groups.set(groupKey, []);
    }

    groups.get(groupKey).push(item);
  }

  for (const groupedItems of groups.values()) {
    const firstItem = groupedItems[0];
    const ownerUid = firstItem.ownerUid || uid;
    const homeId = firstItem.homeId;
    const homeName = firstItem.homeName || homeId;
    const flowType =
      getAlarmIncidentFlowType(groupedItems);

    // SOS/khói/gas/ngập luôn được nhận.
    const enabled = flowType === "emergency"
      ? true
      : await canReceiveAlarm(
          uid,
          homeId,
          ownerUid,
        );

    if (!enabled) {
      console.log(
        "🔕 ALARM INCIDENT NOT CREATED:",
        uid,
        homeId,
        flowType,
      );
      continue;
    }

    const targetKey = getAlarmIncidentTargetKey(
      uid,
      ownerUid,
      homeId,
      flowType,
    );

    const active = await getActiveAlarmIncident(
      uid,
      targetKey,
    );

    if (active) {
      const now = Date.now();
      const activeDetectedAt = Number(
        active.incident.detectedAt ||
        active.incident.createdAt ||
        0,
      );

      const activeAgeMs = activeDetectedAt > 0
        ? now - activeDetectedAt
        : Number.POSITIVE_INFINITY;

      const mergeWindowMs =
        flowType === "emergency"
          ? EMERGENCY_MERGE_WINDOW_MS
          : SECURITY_MERGE_WINDOW_MS;

      const mayMerge =
        activeAgeMs >= 0 &&
        activeAgeMs <= mergeWindowMs;

      if (mayMerge) {
        const mergedItems = normalizeAlarmIncidentItems([
          ...(Array.isArray(active.incident.items)
            ? active.incident.items
            : []),
          ...groupedItems,
        ]);

        await db
          .ref(
            `accounts/${uid}/alarmIncidents/${active.incidentId}`,
          )
          .update({
            items: mergedItems,
            reasons: mergedItems.map((item) => item.reason),
            updatedAt: now,
          });

        console.log(
          "➕ ALARM INCIDENT MERGED:",
          uid,
          active.incidentId,
          flowType,
          mergedItems.length,
        );

        continue;
      }

      // Incident cũ vẫn active có thể đã ở cấp cao hơn.
      // Không được để lần kích hoạt mới bị gộp mãi vào incident cũ,
      // vì như vậy notification và các cấp sau sẽ không chạy lại.
      clearAlarmIncidentTimers(
        uid,
        active.incidentId,
      );

      await db
        .ref(
          `accounts/${uid}/alarmIncidents/${active.incidentId}`,
        )
        .update({
          status: "superseded",
          supersededAt: now,
          supersededReason:
            flowType === "emergency"
              ? "new_emergency_trigger"
              : "new_security_trigger",
          callStatus:
            active.incident.callStatus === "not_started"
              ? "not_started"
              : "superseded",
          updatedAt: now,
        });

      console.log(
        "🔁 OLD ALARM INCIDENT SUPERSEDED:",
        uid,
        active.incidentId,
        flowType,
        `age=${Math.round(activeAgeMs / 1000)}s`,
      );
    }

    const incidentRef = db
      .ref(`accounts/${uid}/alarmIncidents`)
      .push();

    const incidentId = incidentRef.key;

    if (!incidentId) {
      continue;
    }

    const now = Date.now();
    const initialStage =
      flowType === "emergency"
        ? "notification"
        : "detected";

    const incident = {
      incidentId,
      targetKey,
      receiverUid: uid,
      ownerUid,
      homeId,
      homeName,
      flowType,
      status: "active",
      stage: initialStage,
      items: groupedItems,
      reasons: groupedItems.map((item) => item.reason),
      detectedAt: now,
      expireAt:
        now + ALARM_INCIDENT_AUTO_EXPIRE_MS,
      callStatus: "not_started",
      homeSirenStatus: "not_started",
      createdAt: now,
      updatedAt: now,
    };

    if (flowType === "emergency") {
      incident.fullscreenDueAt =
        now + EMERGENCY_FULLSCREEN_DELAY_MS;
      incident.callDueAt =
        now + EMERGENCY_CALL_DELAY_MS;
    } else {
      incident.alarmDueAt =
        now + ALARM_INCIDENT_ALARM_DELAY_MS;
      incident.sirenDueAt =
        now + ALARM_INCIDENT_SIREN_DELAY_MS;
      incident.callDueAt =
        now + ALARM_INCIDENT_CALL_DELAY_MS;
    }

    await db.ref().update({
      [`accounts/${uid}/alarmIncidents/${incidentId}`]:
        incident,
      [`accounts/${uid}/activeAlarmIncidentByTarget/${targetKey}`]:
        incidentId,
    });

    await addHomeNotificationFromBackend({
      uid,
      homeId,
      homeName,
      type: flowType === "emergency"
        ? "emergency_detected"
        : "alarm_detected",
      category: "alarm",
      severity: flowType === "emergency"
        ? "critical"
        : "warning",
      title: flowType === "emergency"
        ? getEmergencyIncidentTitle(groupedItems)
        : "Phát hiện bất thường",
      message:
        groupedItems.map((item) => item.reason).join(", "),
      entityType: "home",
      entityId: homeId,
    });

    await sendAlarmStageSummary(
      uid,
      groupedItems,
      {
        incidentId,
        stage: initialStage,
        flowType,
      },
    );

    scheduleAlarmIncidentStages(
      uid,
      incidentId,
      incident,
    );

    if (flowType === "emergency") {
      console.log(
        "🆘 EMERGENCY INCIDENT DETECTED:",
        uid,
        incidentId,
        homeId,
        `fullscreen=${EMERGENCY_FULLSCREEN_DELAY_MS / 1000}s`,
        `call=${EMERGENCY_CALL_DELAY_MS / 1000}s`,
      );
    } else {
      console.log(
        "🔎 ALARM INCIDENT DETECTED:",
        uid,
        incidentId,
        homeId,
        `alarm=${ALARM_INCIDENT_ALARM_DELAY_MS / 1000}s`,
        `siren=${ALARM_INCIDENT_SIREN_DELAY_MS / 1000}s`,
        `call=${ALARM_INCIDENT_CALL_DELAY_MS / 1000}s`,
      );
    }
  }
}

async function resumeActiveAlarmIncidents() {
  try {
    const accountsSnap = await db
      .ref("accounts")
      .once("value");

    const accounts = accountsSnap.val() || {};
    let resumed = 0;

    for (const [uid, account] of Object.entries(accounts)) {
      const incidents = account?.alarmIncidents || {};

      for (const [incidentId, incident] of Object.entries(incidents)) {
        if (incident?.status !== "active") {
          continue;
        }

        scheduleAlarmIncidentStages(
          uid,
          incidentId,
          incident,
        );
        resumed++;
      }
    }

    console.log(
      "🚨 ACTIVE ALARM INCIDENTS RESUMED:",
      resumed,
    );
  } catch (err) {
    console.log(
      "ALARM INCIDENT RESUME ERROR:",
      err.message,
    );
  }
}

async function resolveAlarmIncidentsForHome({
  ownerUid,
  homeId,
  resolvedBy,
  action,
}) {
  const accountsSnap = await db
    .ref("accounts")
    .once("value");

  const accounts = accountsSnap.val() || {};
  const updates = {};
  const resolved = [];
  const now = Date.now();

  for (const [uid, account] of Object.entries(accounts)) {
    const incidents = account?.alarmIncidents || {};

    for (const [incidentId, incident] of Object.entries(incidents)) {
      if (
        incident?.status !== "active" ||
        String(incident.ownerUid || "") !== ownerUid ||
        String(incident.homeId || "") !== homeId
      ) {
        continue;
      }

      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/status`
      ] = "resolved";
      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/stage`
      ] = "resolved";
      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/resolvedAt`
      ] = now;
      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/resolvedBy`
      ] = resolvedBy;
      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/resolutionAction`
      ] = action;
      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/updatedAt`
      ] = now;
      updates[
        `accounts/${uid}/alarmIncidents/${incidentId}/homeSirenStatus`
      ] = "stop_requested";

      const targetKey = String(
        incident.targetKey || "",
      ).trim();

      if (targetKey) {
        updates[
          `accounts/${uid}/activeAlarmIncidentByTarget/${targetKey}`
        ] = null;
      }

      resolved.push({
        uid,
        incidentId,
      });
    }
  }

  if (resolved.length === 0) {
    return 0;
  }

  await db.ref().update(updates);

  for (const item of resolved) {
    clearAlarmIncidentTimers(
      item.uid,
      item.incidentId,
    );

    await sendAlarmResolvedPush({
      uid: item.uid,
      incidentId: item.incidentId,
      homeId,
      resolvedBy,
      action,
    });
  }

  console.log(
    "✅ ALARM INCIDENTS RESOLVED:",
    ownerUid,
    homeId,
    resolved.length,
    action,
  );

  return resolved.length;
}

async function sendAlarmSummary(uid, items) {
  return sendAlarmStageSummary(
    uid,
    items,
    {
      stage: "alarm",
    },
  );
}

function queueEventAlarm(uid, item) {
  const flowType = getAlarmIncidentFlowType([item]);

  if (flowType === "emergency") {
    void startOrMergeAlarmIncidents(
      uid,
      [item],
    ).catch((error) => {
      console.log(
        "EMERGENCY INCIDENT START ERROR:",
        uid,
        error.message,
      );
    });

    return;
  }

  if (!pendingEventAlarmMap[uid]) {
    pendingEventAlarmMap[uid] = [];
  }

  const exists = pendingEventAlarmMap[uid].some((oldItem) => {
    return (
      oldItem.homeId === item.homeId &&
      oldItem.reason === item.reason
    );
  });

  if (!exists) {
    pendingEventAlarmMap[uid].push(item);
  }

  if (pendingEventAlarmTimerMap[uid]) {
    return;
  }

  pendingEventAlarmTimerMap[uid] = setTimeout(
    async () => {
      const items = pendingEventAlarmMap[uid] || [];

      delete pendingEventAlarmMap[uid];
      delete pendingEventAlarmTimerMap[uid];

      await startOrMergeAlarmIncidents(uid, items);
    },
    1200,
  );
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

function isSecurityDeviceType(deviceType) {
  return (
    deviceType === "door" ||
    deviceType === "window" ||
    deviceType === "gate" ||
    deviceType === "lock" ||
    deviceType === "door_lock" ||
    deviceType === "motion" ||
    deviceType === "presence" ||
    deviceType === "vibration" ||
    deviceType === "glass_break"
  );
}

function isEmergencyDeviceType(deviceType) {
  return (
    deviceType === "smoke" ||
    deviceType === "heat" ||
    deviceType === "carbon_monoxide" ||
    deviceType === "gas" ||
    deviceType === "water_leak" ||
    deviceType === "flood" ||
    deviceType === "sos"
  );
}

function normalizeRepeatMinutes(value) {
  const minutes = Number.parseInt(value || 0, 10);

  if (!Number.isFinite(minutes) || minutes < 0) {
    return 0;
  }

  return minutes;
}

function getScheduleAlarmKey(
  receiverUid,
  ownerUid,
  homeId,
  deviceId,
  alarm,
) {
  return [
    receiverUid,
    ownerUid,
    homeId,
    deviceId,
    String(alarm?.start || ""),
    String(alarm?.end || ""),
  ].join("_");
}

async function resolveDeviceAlarmForReceiver(
  receiverUid,
  homeId,
  deviceId,
  homeData,
  receiverAccount = null,
) {
  let deviceAlarm =
    homeData.devices?.[deviceId]?.alarm || null;

  try {
    let mode = "home";
    let customAlarm = null;

    if (receiverAccount) {
      const customHomeRules =
        receiverAccount.customRules?.[homeId] || {};

      mode = String(customHomeRules.mode || "home");
      customAlarm =
        customHomeRules.devices?.[deviceId]?.alarm || null;
    } else {
      const modeSnap = await db
        .ref(
          `accounts/${receiverUid}/customRules/${homeId}/mode`,
        )
        .once("value");

      mode = String(modeSnap.val() || "home");

      if (mode === "custom") {
        const customAlarmSnap = await db
          .ref(
            `accounts/${receiverUid}/customRules/${homeId}/devices/${deviceId}/alarm`,
          )
          .once("value");

        customAlarm = customAlarmSnap.val();
      }
    }

    // Chế độ Riêng tôi chỉ ghi đè sensor đã có cài đặt riêng.
    // Sensor chưa có cài đặt riêng tiếp tục kế thừa lịch của nhà.
    if (mode === "custom" && customAlarm) {
      deviceAlarm = customAlarm;
    }
  } catch (error) {
    console.log(
      "CUSTOM DEVICE ALARM LOAD ERROR:",
      receiverUid,
      homeId,
      deviceId,
      error.message,
    );
  }

  if (!deviceAlarm || typeof deviceAlarm !== "object") {
    return null;
  }

  return deviceAlarm;
}

function getUnsafeSecurityReason(
  deviceName,
  deviceType,
  device,
) {
  if (device.tamper === true) {
    return `${deviceName}: Thiết bị bị tháo`;
  }

  if (
    (
      deviceType === "motion" ||
      deviceType === "presence"
    ) &&
    (
      isActiveSignal(device.occupancy) ||
      isActiveSignal(device.motion) ||
      isActiveSignal(device.presence)
    )
  ) {
    return `${deviceName}: Phát hiện chuyển động`;
  }

  if (
    (
      deviceType === "door_lock" ||
      deviceType === "lock"
    ) &&
    normalizeLockState(device) === "unlocked"
  ) {
    return `${deviceName}: Khóa đang mở`;
  }

  if (
    (
      deviceType === "door" ||
      deviceType === "window" ||
      deviceType === "gate"
    ) &&
    device.contact === false
  ) {
    return `${deviceName}: Cửa đang mở`;
  }

  return "";
}

async function processScheduleAlarmsForOwner(
  receiverUid,
  ownerUid,
  homeId,
  homeName,
  deviceId,
  deviceName,
  deviceType,
  homeData,
  updateData,
) {
  const oldDevice =
    homeData.devices?.[deviceId] || {};

  if (
    !isSecurityDeviceType(deviceType) &&
    !isEmergencyDeviceType(deviceType)
  ) {
    return;
  }

  // Khói, SOS, gas và ngập luôn cảnh báo,
  // không phụ thuộc Mode hoặc lịch của sensor.
  if (isEmergencyDeviceType(deviceType)) {
    if (
      deviceType === "smoke" &&
      updateData.smoke === true &&
      oldDevice.smoke !== true
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
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

    if (
      deviceType === "sos" &&
      updateData.action !== undefined
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
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

    if (
      deviceType === "heat" &&
      (
        (
          isActiveSignal(updateData.heat) &&
          !isActiveSignal(oldDevice.heat)
        ) ||
        (
          isActiveSignal(updateData.heat_alarm) &&
          !isActiveSignal(oldDevice.heat_alarm)
        ) ||
        (
          isActiveSignal(
            updateData.high_temperature_alarm,
          ) &&
          !isActiveSignal(
            oldDevice.high_temperature_alarm,
          )
        )
      )
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Phát hiện nhiệt độ nguy hiểm`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });

      return;
    }

    if (
      deviceType === "carbon_monoxide" &&
      (
        (
          isActiveSignal(
            updateData.carbon_monoxide,
          ) &&
          !isActiveSignal(
            oldDevice.carbon_monoxide,
          )
        ) ||
        (
          isActiveSignal(updateData.co_alarm) &&
          !isActiveSignal(oldDevice.co_alarm)
        )
      )
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Phát hiện khí CO`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });

      return;
    }

    if (
      deviceType === "gas" &&
      (
        (
          isActiveSignal(updateData.gas) &&
          !isActiveSignal(oldDevice.gas)
        ) ||
        (
          isActiveSignal(updateData.gas_alarm) &&
          !isActiveSignal(oldDevice.gas_alarm)
        )
      )
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Phát hiện rò rỉ gas`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });

      return;
    }

    if (
      (
        deviceType === "water_leak" ||
        deviceType === "flood"
      ) &&
      (
        (
          isActiveSignal(updateData.water_leak) &&
          !isActiveSignal(oldDevice.water_leak)
        ) ||
        (
          isActiveSignal(updateData.leak) &&
          !isActiveSignal(oldDevice.leak)
        ) ||
        (
          isActiveSignal(updateData.water) &&
          !isActiveSignal(oldDevice.water)
        )
      )
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Phát hiện ngập nước`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });

      return;
    }

    if (
      updateData.tamper === true &&
      oldDevice.tamper !== true
    ) {
      queueEventAlarm(receiverUid, {
        ownerUid,
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason: `${deviceName}: Thiết bị bị tháo`,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
      });
    }

    return;
  }

  const deviceAlarm =
    await resolveDeviceAlarmForReceiver(
      receiverUid,
      homeId,
      deviceId,
      homeData,
    );

  const manualArmed =
    homeData.securityMode === "armed";

  const scheduleArmed =
    deviceAlarm?.enabled === true &&
    isNowInRange(
      deviceAlarm.start,
      deviceAlarm.end,
    );

  // Mode Bảo vệ thủ công bảo vệ toàn bộ sensor.
  // Mode Bình thường chỉ bảo vệ sensor đang nằm trong lịch riêng.
  if (!manualArmed && !scheduleArmed) {
    return;
  }

  const repeatMinutes = scheduleArmed
    ? normalizeRepeatMinutes(
        deviceAlarm?.repeatMinutes,
      )
    : 0;

  const nextAlarm = scheduleArmed
    ? getNextAlarmTimeText(repeatMinutes)
    : "không lặp lại";

  function rememberScheduleTrigger() {
    if (!scheduleArmed) {
      return;
    }

    const alarmKey = getScheduleAlarmKey(
      receiverUid,
      ownerUid,
      homeId,
      deviceId,
      deviceAlarm,
    );

    lastScheduleAlarmMap[alarmKey] = Date.now();
  }

  if (
    updateData.contact === false &&
    oldDevice.contact !== false
  ) {
    rememberScheduleTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Cửa mở bất thường`,
      repeatMinutes,
      nextAlarm,
    });

    return;
  }

  if (
    updateData.tamper === true &&
    oldDevice.tamper !== true
  ) {
    rememberScheduleTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Thiết bị bị tháo`,
      repeatMinutes,
      nextAlarm,
    });

    return;
  }

  const motionTriggered =
    (
      isActiveSignal(updateData.occupancy) &&
      !isActiveSignal(oldDevice.occupancy)
    ) ||
    (
      isActiveSignal(updateData.motion) &&
      !isActiveSignal(oldDevice.motion)
    ) ||
    (
      isActiveSignal(updateData.presence) &&
      !isActiveSignal(oldDevice.presence)
    );

  if (
    (
      deviceType === "motion" ||
      deviceType === "presence"
    ) &&
    motionTriggered
  ) {
    rememberScheduleTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Phát hiện chuyển động`,
      repeatMinutes,
      nextAlarm,
    });

    return;
  }

  if (
    (
      deviceType === "vibration" ||
      deviceType === "glass_break"
    ) &&
    (
      isActiveSignal(updateData.vibration) ||
      updateData.action !== undefined
    )
  ) {
    rememberScheduleTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason:
        deviceType === "glass_break"
          ? `${deviceName}: Phát hiện kính vỡ`
          : `${deviceName}: Phát hiện rung/chấn động`,
      repeatMinutes,
      nextAlarm,
    });

    return;
  }

  if (
    (
      deviceType === "door_lock" ||
      deviceType === "lock"
    ) &&
    normalizeLockState({
      ...oldDevice,
      ...updateData,
    }) === "unlocked" &&
    normalizeLockState(oldDevice) !== "unlocked"
  ) {
    rememberScheduleTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Khóa đã mở`,
      repeatMinutes,
      nextAlarm,
    });
  }
}

async function checkScheduledAlarms() {
  console.log("🚨 CHECK PER-DEVICE ALARM SCHEDULE");

  try {
    const snap = await db
      .ref("accounts")
      .once("value");

    const accounts = snap.val() || {};
    const now = Date.now();
    const alarmSummaryByUser = {};

    for (const [receiverUid, receiverAccount] of Object.entries(accounts)) {
      const ownHomes = receiverAccount?.homes || {};
      const sharedHomes = receiverAccount?.sharedHomes || {};
      const homesToCheck = [];

      for (const [homeId, home] of Object.entries(ownHomes)) {
        homesToCheck.push({
          receiverUid,
          ownerUid: receiverUid,
          homeId,
          home,
        });
      }

      for (const [homeId, sharedInfo] of Object.entries(sharedHomes)) {
        const ownerUid = String(
          sharedInfo?.ownerUid || "",
        ).trim();

        if (!ownerUid) {
          continue;
        }

        const sharedHome =
          accounts[ownerUid]?.homes?.[homeId];

        if (!sharedHome) {
          continue;
        }

        homesToCheck.push({
          receiverUid,
          ownerUid,
          homeId,
          home: sharedHome,
        });
      }

      for (const item of homesToCheck) {
        const {
          ownerUid,
          homeId,
          home,
        } = item;

        const canReceive = await canReceiveAlarm(
          receiverUid,
          homeId,
          ownerUid,
        );

        const devices = home?.devices || {};

        for (const [deviceId, device] of Object.entries(devices)) {
          const deviceType = String(
            device?.type || "door",
          ).trim();

          if (!isSecurityDeviceType(deviceType)) {
            continue;
          }

          const deviceAlarm =
            await resolveDeviceAlarmForReceiver(
              receiverUid,
              homeId,
              deviceId,
              home,
              receiverAccount,
            );

          if (!deviceAlarm || deviceAlarm.enabled !== true) {
            continue;
          }

          const alarmKey = getScheduleAlarmKey(
            receiverUid,
            ownerUid,
            homeId,
            deviceId,
            deviceAlarm,
          );

          if (
            !isNowInRange(
              deviceAlarm.start,
              deviceAlarm.end,
            )
          ) {
            delete lastScheduleAlarmMap[alarmKey];
            continue;
          }

          if (!canReceive) {
            continue;
          }

          const deviceName =
            String(device?.name || deviceId);

          const reason = getUnsafeSecurityReason(
            deviceName,
            deviceType,
            device || {},
          );

          if (!reason) {
            delete lastScheduleAlarmMap[alarmKey];
            continue;
          }

          const repeatMinutes =
            normalizeRepeatMinutes(
              deviceAlarm.repeatMinutes,
            );

          const lastTime =
            lastScheduleAlarmMap[alarmKey] || 0;

          if (repeatMinutes === 0 && lastTime > 0) {
            continue;
          }

          if (
            repeatMinutes > 0 &&
            lastTime > 0 &&
            now - lastTime < repeatMinutes * 60 * 1000
          ) {
            continue;
          }

          lastScheduleAlarmMap[alarmKey] = now;

          if (!alarmSummaryByUser[receiverUid]) {
            alarmSummaryByUser[receiverUid] = [];
          }

          alarmSummaryByUser[receiverUid].push({
            ownerUid,
            homeId,
            homeName: home?.name || homeId,
            deviceName,
            type: deviceType,
            reason,
            repeatMinutes,
            nextAlarm:
              getNextAlarmTimeText(repeatMinutes),
          });
        }
      }
    }

    for (const [receiverUid, items] of Object.entries(alarmSummaryByUser)) {
      await startOrMergeAlarmIncidents(
        receiverUid,
        items,
      );
    }
  } catch (error) {
    console.log(
      "PER-DEVICE ALARM SCHEDULE ERROR:",
      error.message,
    );
  }
}

async function cleanupLegacySecurityScheduleState() {
  try {
    const snap = await db
      .ref("accounts")
      .once("value");

    const accounts = snap.val() || {};
    const updates = {};

    for (const [ownerUid, account] of Object.entries(accounts)) {
      const homes = account?.homes || {};

      for (const [homeId, home] of Object.entries(homes)) {
        if (home?.securityModeSource === "schedule") {
          updates[
            `accounts/${ownerUid}/homes/${homeId}/securityMode`
          ] = "normal";

          updates[
            `accounts/${ownerUid}/homes/${homeId}/securityModeSource`
          ] = null;
        }

        if (
          Object.prototype.hasOwnProperty.call(
            home || {},
            "securityScheduleActive",
          )
        ) {
          updates[
            `accounts/${ownerUid}/homes/${homeId}/securityScheduleActive`
          ] = null;
        }
      }
    }

    if (Object.keys(updates).length === 0) {
      return;
    }

    await db.ref().update(updates);

    console.log(
      "🧹 LEGACY SECURITY SCHEDULE STATE CLEARED:",
      Object.keys(updates).length,
    );
  } catch (error) {
    console.log(
      "LEGACY SECURITY SCHEDULE CLEANUP ERROR:",
      error.message,
    );
  }
}


// ================= SECURITY MODE TRANSITION =================
// Khi một nhà chuyển từ normal -> armed, phải kiểm tra ngay trạng thái
// hiện tại của toàn bộ sensor. Nhờ vậy cửa/khóa đã mở từ trước vẫn tạo
// Alarm mà không cần chờ một MQTT event mới.
const securityModeHomeListenerMap = new Map();
const securityModeAccountListenerMap = new Map();
const securityModeLastValueMap = new Map();
const securityModeTransitionInProgress = new Set();

function getSecurityModeHomeKey(ownerUid, homeId) {
  return `${ownerUid}|${homeId}`;
}

function normalizeHomeSecurityMode(value) {
  return String(value || "").trim() === "armed"
    ? "armed"
    : "normal";
}

function detachSecurityModeHomeListener(ownerUid, homeId) {
  const key = getSecurityModeHomeKey(ownerUid, homeId);
  const listener = securityModeHomeListenerMap.get(key);

  if (listener) {
    listener.ref.off("value", listener.callback);
    securityModeHomeListenerMap.delete(key);
  }

  securityModeLastValueMap.delete(key);
  securityModeTransitionInProgress.delete(key);
}

async function triggerAlarmForUnsafeStateOnArmed(
  ownerUid,
  homeId,
) {
  const transitionKey = getSecurityModeHomeKey(
    ownerUid,
    homeId,
  );

  if (securityModeTransitionInProgress.has(transitionKey)) {
    return;
  }

  securityModeTransitionInProgress.add(transitionKey);

  try {
    const [homeSnap, sharedSnap] = await Promise.all([
      db
        .ref(`accounts/${ownerUid}/homes/${homeId}`)
        .once("value"),
      db
        .ref(`sharedByHome/${homeId}`)
        .once("value"),
    ]);

    const home = homeSnap.val();

    // Mode có thể đã được đổi ngược về normal trong lúc đang đọc dữ liệu.
    if (
      !home ||
      normalizeHomeSecurityMode(home.securityMode) !== "armed"
    ) {
      return;
    }

    const homeName = String(home.name || homeId).trim() || homeId;
    const devices = asObject(home.devices);
    const alarmItems = [];

    for (const [deviceId, rawDevice] of Object.entries(devices)) {
      const device = asObject(rawDevice);
      const deviceType = String(
        device.type || "unknown",
      ).trim();

      if (!isSecurityDeviceType(deviceType)) {
        continue;
      }

      const deviceName = String(
        device.name || deviceId,
      ).trim() || deviceId;

      const reason = getUnsafeSecurityReason(
        deviceName,
        deviceType,
        device,
      );

      if (!reason) {
        continue;
      }

      alarmItems.push({
        ownerUid,
        homeId,
        homeName,
        deviceName,
        type: deviceType,
        reason,
        repeatMinutes: 0,
        nextAlarm: "không lặp lại",
      });
    }

    if (alarmItems.length === 0) {
      console.log(
        "🛡️ SECURITY MODE ARMED, CURRENT STATE SAFE:",
        ownerUid,
        homeId,
      );
      return;
    }

    const receiverUids = new Set([ownerUid]);
    const sharedMembers = asObject(sharedSnap.val());

    for (const sharedUid of Object.keys(sharedMembers)) {
      const cleanUid = String(sharedUid || "").trim();

      if (cleanUid) {
        receiverUids.add(cleanUid);
      }
    }

    for (const receiverUid of receiverUids) {
      await startOrMergeAlarmIncidents(
        receiverUid,
        alarmItems,
      );
    }

    console.log(
      "🚨 SECURITY MODE ARMED WITH EXISTING UNSAFE STATE:",
      ownerUid,
      homeId,
      `items=${alarmItems.length}`,
      `receivers=${receiverUids.size}`,
    );
  } catch (error) {
    console.log(
      "SECURITY MODE TRANSITION ALARM ERROR:",
      ownerUid,
      homeId,
      error.message,
    );
  } finally {
    securityModeTransitionInProgress.delete(transitionKey);
  }
}

function attachSecurityModeHomeListener(ownerUid, homeId) {
  const key = getSecurityModeHomeKey(ownerUid, homeId);

  if (securityModeHomeListenerMap.has(key)) {
    return;
  }

  const modeRef = db.ref(
    `accounts/${ownerUid}/homes/${homeId}/securityMode`,
  );

  const callback = (snapshot) => {
    const nextMode = normalizeHomeSecurityMode(
      snapshot.val(),
    );

    if (!securityModeLastValueMap.has(key)) {
      // Lần đọc đầu chỉ dùng để lấy trạng thái nền, không tạo Alarm
      // khi backend vừa restart trong lúc nhà đã ở mode Bảo vệ.
      securityModeLastValueMap.set(key, nextMode);
      return;
    }

    const previousMode = securityModeLastValueMap.get(key);
    securityModeLastValueMap.set(key, nextMode);

    if (
      previousMode !== "armed" &&
      nextMode === "armed"
    ) {
      void triggerAlarmForUnsafeStateOnArmed(
        ownerUid,
        homeId,
      );
    }
  };

  modeRef.on(
    "value",
    callback,
    (error) => {
      console.log(
        "SECURITY MODE HOME LISTENER ERROR:",
        ownerUid,
        homeId,
        error.message,
      );
    },
  );

  securityModeHomeListenerMap.set(key, {
    ref: modeRef,
    callback,
  });
}

function detachSecurityModeAccountListener(ownerUid) {
  const listener = securityModeAccountListenerMap.get(ownerUid);

  if (listener) {
    listener.ref.off("child_added", listener.onHomeAdded);
    listener.ref.off("child_removed", listener.onHomeRemoved);
    securityModeAccountListenerMap.delete(ownerUid);
  }

  for (const key of Array.from(securityModeHomeListenerMap.keys())) {
    if (!key.startsWith(`${ownerUid}|`)) {
      continue;
    }

    const homeId = key.slice(ownerUid.length + 1);
    detachSecurityModeHomeListener(ownerUid, homeId);
  }
}

function attachSecurityModeAccountListener(ownerUid) {
  if (securityModeAccountListenerMap.has(ownerUid)) {
    return;
  }

  const homesRef = db.ref(`accounts/${ownerUid}/homes`);

  const onHomeAdded = (homeSnapshot) => {
    const homeId = String(homeSnapshot.key || "").trim();

    if (homeId) {
      attachSecurityModeHomeListener(ownerUid, homeId);
    }
  };

  const onHomeRemoved = (homeSnapshot) => {
    const homeId = String(homeSnapshot.key || "").trim();

    if (homeId) {
      detachSecurityModeHomeListener(ownerUid, homeId);
    }
  };

  homesRef.on("child_added", onHomeAdded);
  homesRef.on("child_removed", onHomeRemoved);

  securityModeAccountListenerMap.set(ownerUid, {
    ref: homesRef,
    onHomeAdded,
    onHomeRemoved,
  });
}

async function startSecurityModeTransitionMonitor() {
  const accountsRef = db.ref("accounts");

  // Chụp trạng thái nền trước khi Auto Away bắt đầu chạy. Nhờ đó lần
  // normal -> armed đầu tiên sau khi backend khởi động không bị bỏ lỡ.
  const accountsSnap = await accountsRef.once("value");
  const accounts = asObject(accountsSnap.val());

  for (const [ownerUid, rawAccount] of Object.entries(accounts)) {
    const homes = asObject(rawAccount?.homes);

    for (const [homeId, rawHome] of Object.entries(homes)) {
      const key = getSecurityModeHomeKey(ownerUid, homeId);
      const home = asObject(rawHome);

      securityModeLastValueMap.set(
        key,
        normalizeHomeSecurityMode(home.securityMode),
      );
    }

    attachSecurityModeAccountListener(ownerUid);
  }

  accountsRef.on("child_added", (accountSnapshot) => {
    const ownerUid = String(accountSnapshot.key || "").trim();

    if (ownerUid) {
      attachSecurityModeAccountListener(ownerUid);
    }
  });

  accountsRef.on("child_removed", (accountSnapshot) => {
    const ownerUid = String(accountSnapshot.key || "").trim();

    if (ownerUid) {
      detachSecurityModeAccountListener(ownerUid);
    }
  });

  console.log(
    "🛡️ SECURITY MODE TRANSITION MONITOR STARTED:",
    `homes=${securityModeLastValueMap.size}`,
  );
}

// ================= AUTO AWAY =================
const AUTO_AWAY_ARM_DELAY_MS = 30 * 1000;
const AUTO_AWAY_SCAN_INTERVAL_MS = 10 * 1000;

let autoAwayTimer = null;
let autoAwayScanRunning = false;

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value
    : {};
}

function normalizePresenceState(
  accounts,
  memberUid,
  ownerUid,
  homeId,
) {
  const presence = asObject(
    accounts?.[memberUid]?.homePresence?.[homeId],
  );

  const storedOwnerUid = String(
    presence.ownerUid || "",
  ).trim();

  const storedHomeId = String(
    presence.homeId || "",
  ).trim();

  const state = String(
    presence.state || "unknown",
  ).trim();

  if (
    storedOwnerUid !== ownerUid ||
    storedHomeId !== homeId ||
    (state !== "inside" && state !== "outside")
  ) {
    return "unknown";
  }

  return state;
}

function runtimeSignature(runtime) {
  const value = asObject(runtime);

  return JSON.stringify({
    status: String(value.status || ""),
    memberCount: Number(value.memberCount || 0),
    insideCount: Number(value.insideCount || 0),
    outsideCount: Number(value.outsideCount || 0),
    unknownCount: Number(value.unknownCount || 0),
    allOutsideSince: Number(value.allOutsideSince || 0),
    cycleArmed: value.cycleArmed === true,
  });
}

function buildRuntime({
  status,
  memberCount,
  insideCount,
  outsideCount,
  unknownCount,
  allOutsideSince,
  cycleArmed,
  now,
}) {
  return {
    status,
    memberCount,
    insideCount,
    outsideCount,
    unknownCount,
    allOutsideSince: allOutsideSince || null,
    cycleArmed: cycleArmed === true,
    updatedAt: now,
  };
}


function presenceSummarySignature(summary) {
  const value = asObject(summary);

  return JSON.stringify({
    memberCount: Number(value.memberCount || 0),
    insideCount: Number(value.insideCount || 0),
    outsideCount: Number(value.outsideCount || 0),
    unknownCount: Number(value.unknownCount || 0),
  });
}

function buildPresenceSummary({
  memberCount,
  insideCount,
  outsideCount,
  unknownCount,
  now,
}) {
  return {
    memberCount,
    insideCount,
    outsideCount,
    unknownCount,
    updatedAt: now,
  };
}

async function checkAutoAwayHomes(db) {
  if (autoAwayScanRunning) {
    return;
  }

  autoAwayScanRunning = true;

  try {
    const [accountsSnap, sharedByHomeSnap] =
      await Promise.all([
        db.ref("accounts").once("value"),
        db.ref("sharedByHome").once("value"),
      ]);

    const accounts = accountsSnap.val() || {};
    const sharedByHome = sharedByHomeSnap.val() || {};
    const now = Date.now();
    const updates = {};
    const logs = [];

    for (const [ownerUid, rawAccount] of Object.entries(accounts)) {
      const account = asObject(rawAccount);
      const homes = asObject(account.homes);

      for (const [homeId, rawHome] of Object.entries(homes)) {
        const home = asObject(rawHome);
        const autoAway = asObject(home.autoAway);
        const runtime = asObject(home.autoAwayRuntime);

        const homePath =
          `accounts/${ownerUid}/homes/${homeId}`;
        const runtimePath =
          `${homePath}/autoAwayRuntime`;
        const presenceSummaryPath =
          `${homePath}/presenceSummary`;

        const members = new Set([ownerUid]);
        const sharedMembers = asObject(sharedByHome[homeId]);

        for (const memberUid of Object.keys(sharedMembers)) {
          if (memberUid.trim() !== "") {
            members.add(memberUid);
          }
        }

        const states = [];

        for (const memberUid of members) {
          states.push(
            normalizePresenceState(
              accounts,
              memberUid,
              ownerUid,
              homeId,
            ),
          );
        }

        const memberCount = states.length;
        const insideCount = states.filter(
          (state) => state === "inside",
        ).length;
        const outsideCount = states.filter(
          (state) => state === "outside",
        ).length;
        const unknownCount =
          memberCount - insideCount - outsideCount;

        const currentPresenceSummary =
          asObject(home.presenceSummary);

        const nextPresenceSummary =
          buildPresenceSummary({
            memberCount,
            insideCount,
            outsideCount,
            unknownCount,
            now,
          });

        if (
          presenceSummarySignature(currentPresenceSummary) !==
          presenceSummarySignature(nextPresenceSummary)
        ) {
          updates[presenceSummaryPath] =
            nextPresenceSummary;
        }

        if (autoAway.enabled !== true) {
          if (Object.keys(runtime).length > 0) {
            updates[runtimePath] = null;
          }

          if (
            home.securityMode === "armed" &&
            home.securityModeSource === "auto_away"
          ) {
            updates[`${homePath}/securityMode`] = "normal";
            updates[`${homePath}/securityModeSource`] = null;

            logs.push(
              `🏠 AUTO AWAY OFF → NORMAL: ${ownerUid} ${homeId}`,
            );
          }

          continue;
        }

        const anyInside = insideCount > 0;
        const allOutside =
          memberCount > 0 && outsideCount === memberCount;

        let nextRuntime;

        if (anyInside) {
          nextRuntime = buildRuntime({
            status: "inside",
            memberCount,
            insideCount,
            outsideCount,
            unknownCount,
            allOutsideSince: 0,
            cycleArmed: false,
            now,
          });

          if (
            home.securityMode === "armed" &&
            home.securityModeSource === "auto_away"
          ) {
            updates[`${homePath}/securityMode`] = "normal";
            updates[`${homePath}/securityModeSource`] = null;

            logs.push(
              `🏠 AUTO AWAY MEMBER RETURNED → NORMAL: ${ownerUid} ${homeId}`,
            );
          }
        } else if (!allOutside) {
          nextRuntime = buildRuntime({
            status: "waiting_presence",
            memberCount,
            insideCount,
            outsideCount,
            unknownCount,
            allOutsideSince: 0,
            cycleArmed: runtime.cycleArmed === true,
            now,
          });
        } else {
          const storedSince = Number(
            runtime.allOutsideSince || 0,
          );

          const allOutsideSince =
            storedSince > 0 ? storedSince : now;

          let cycleArmed = runtime.cycleArmed === true;
          const elapsed = now - allOutsideSince;

          if (
            !cycleArmed &&
            elapsed >= AUTO_AWAY_ARM_DELAY_MS
          ) {
            cycleArmed = true;

            if (home.securityMode !== "armed") {
              updates[`${homePath}/securityMode`] = "armed";
              updates[`${homePath}/securityModeSource`] = "auto_away";

              logs.push(
                `🛡️ AUTO AWAY ARMED: ${ownerUid} ${homeId} members=${memberCount}`,
              );
            } else {
              logs.push(
                `🛡️ AUTO AWAY CYCLE READY, MODE ALREADY ARMED: ${ownerUid} ${homeId}`,
              );
            }
          }

          nextRuntime = buildRuntime({
            status: cycleArmed ? "armed" : "countdown",
            memberCount,
            insideCount,
            outsideCount,
            unknownCount,
            allOutsideSince,
            cycleArmed,
            now,
          });
        }

        if (
          runtimeSignature(runtime) !==
          runtimeSignature(nextRuntime)
        ) {
          updates[runtimePath] = nextRuntime;
        }
      }
    }

    if (Object.keys(updates).length > 0) {
      await db.ref().update(updates);
    }

    for (const line of logs) {
      console.log(line);
    }
  } catch (error) {
    console.log(
      "AUTO AWAY MONITOR ERROR:",
      error.message,
    );
  } finally {
    autoAwayScanRunning = false;
  }
}

function startAutoAwayMonitor({ db }) {
  if (!db) {
    throw new Error("AUTO AWAY DB IS REQUIRED");
  }

  if (autoAwayTimer) {
    return;
  }

  console.log(
    "📍 AUTO AWAY MONITOR STARTED:",
    `delay=${AUTO_AWAY_ARM_DELAY_MS / 1000}s`,
    `scan=${AUTO_AWAY_SCAN_INTERVAL_MS / 1000}s`,
  );

  void checkAutoAwayHomes(db);

  autoAwayTimer = setInterval(
    () => {
      void checkAutoAwayHomes(db);
    },
    AUTO_AWAY_SCAN_INTERVAL_MS,
  );
}

// ================= CHAT PUSH =================
async function countUnreadChatMessages(
  homeId,
  receiverUid,
) {
  const [lastReadSnap, messagesSnap] =
    await Promise.all([
      db
        .ref(`homeChats/${homeId}/lastRead/${receiverUid}`)
        .once("value"),

      db
        .ref(`homeChats/${homeId}/messages`)
        .once("value"),
    ]);

  const lastRead =
    Number(lastReadSnap.val() || 0);

  const messages =
    messagesSnap.val() || {};

  let unreadCount = 0;

  for (const rawMessage of Object.values(messages)) {
    const chatMessage = rawMessage || {};
    const senderUid =
      String(chatMessage.uid || "").trim();

    const messageTime =
      Number(chatMessage.time || 0);

    if (
      senderUid &&
      senderUid !== receiverUid &&
      Number.isFinite(messageTime) &&
      messageTime > lastRead
    ) {
      unreadCount++;
    }
  }

  return unreadCount;
}

async function sendChatNotificationPush({
  receiverUid,
  ownerUid,
  homeId,
  homeName,
  senderUid,
  senderName,
  messageId,
  text,
  unreadCount,
}) {
  if (unreadCount <= 0) {
    return;
  }

  const cleanHomeName =
    String(homeName || "").trim() || "HomeChat";

  const cleanSenderName =
    String(senderName || "").trim() ||
    "Một thành viên";

  const cleanText =
    String(text || "").trim();

  const title =
    unreadCount > 1
      ? `${cleanHomeName} · ${unreadCount} tin nhắn mới`
      : cleanHomeName;

  const body =
    `${cleanSenderName}: ${cleanText}`;

  const data = {
    type: "chat",
    title,
    body,
    ownerUid: String(ownerUid || ""),
    homeId: String(homeId || ""),
    homeName: cleanHomeName,
    senderUid: String(senderUid || ""),
    senderName: cleanSenderName,
    messageId: String(messageId || ""),
    unreadCount: String(unreadCount),
    clickAction: "home_chat",
  };

  const pushResult = await sendPushToUser(
    receiverUid,
    {
      data,

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
              title,
              body,
            },
            sound: "default",
            badge: unreadCount,
            threadId: `home_chat_${homeId}`,
          },
        },
      },
    },
    "CHAT",
  );

  if (pushResult.sent === 0) {
    return;
  }

  console.log(
    "💬 CHAT PUSH SENT:",
    receiverUid,
    homeId,
    unreadCount,
    `devices=${pushResult.sent}`,
  );
}

// ================= INIT =================
async function init() {
  startDeviceMapListener();
  startUserDirectoryListener();
  await db.ref("pair_requests").remove();
  console.log("🧹 OLD PAIR REQUESTS CLEARED");

  await cleanupLegacySecurityScheduleState();

  // Khôi phục các sự cố Alarm chưa được xử lý sau khi backend restart.
  await resumeActiveAlarmIncidents();

  // Khi mode chuyển normal -> armed, kiểm tra ngay trạng thái sensor hiện tại.
  await startSecurityModeTransitionMonitor();

  // Tự động chuyển Mode khi toàn bộ thành viên rời nhà.
  startAutoAwayMonitor({ db });

  // Ghi nhịp sống của Raspberry Pi/backend lên Firebase.
  startHubHeartbeat();

  setInterval(cleanupExpiredAlarmPause, 60000);
  setInterval(checkScheduledNotifications, 60000);

  // Kiểm tra lịch Alarm riêng của từng sensor.
  setInterval(checkScheduledAlarms, 60000);
}
const deviceDeleteInProgress = new Set();

const homeNotificationRequestInProgress = new Set();
const lastHomeNotificationRequestMap = {};

db.ref("home_notification_requests").on("child_added", async (snap) => {
  const req = snap.val();
  const requestId = snap.key;

  async function rejectRequest(reason) {
    console.log(
      "❌ HOME NOTIFICATION REQUEST REJECTED:",
      requestId,
      reason,
    );

    try {
      await snap.ref.remove();
    } catch (_) {}
  }

  try {
    if (!req || !requestId) {
      return;
    }

    if (homeNotificationRequestInProgress.has(requestId)) {
      return;
    }

    homeNotificationRequestInProgress.add(requestId);

    const requestedBy = String(
      req.requestedBy || "",
    ).trim();

    const ownerUid = String(
      req.ownerUid || "",
    ).trim();

    const homeId = String(
      req.homeId || "",
    ).trim();

    const recipientUid = String(
      req.recipientUid || "",
    ).trim();

    const type = String(
      req.type || "",
    ).trim();

    const category = String(
      req.category || "",
    ).trim();

    const severity = String(
      req.severity || "",
    ).trim();

    const title = String(
      req.title || "",
    ).trim();

    const message = String(
      req.message || "",
    ).trim();

    const deviceId = String(
      req.deviceId || "",
    ).trim();

    const entityType = String(
      req.entityType || "",
    ).trim();

    const entityId = String(
      req.entityId || "",
    ).trim();

    const requestTime = Number(req.time);
    const now = Date.now();

    const allowedCategories = new Set([
      "home",
      "device",
      "member",
      "alarm",
      "reminder",
      "chat",
    ]);

    const allowedSeverities = new Set([
      "info",
      "success",
      "warning",
    ]);

    const requestTargetedTypes = new Set([
      "share_request",
      "join_request",
      "transfer_owner_request",
    ]);

    const memberTargetedTypes = new Set([
      "share_request_accepted",
      "join_request_accepted",
      "member_join",
      "transfer_owner_accepted",
      "role_changed",
      "member_removed",
    ]);

    const targetedTypes = new Set([
      ...requestTargetedTypes,
      ...memberTargetedTypes,
    ]);

    const invalidRequest =
      req.status !== "pending" ||
      requestedBy.length === 0 ||
      requestedBy.length > 128 ||
      ownerUid.length === 0 ||
      ownerUid.length > 128 ||
      homeId.length === 0 ||
      homeId.length > 120 ||
      recipientUid.length > 128 ||
      type.length === 0 ||
      type.length > 80 ||
      !allowedCategories.has(category) ||
      !allowedSeverities.has(severity) ||
      title.length === 0 ||
      title.length > 160 ||
      message.length === 0 ||
      message.length > 1000 ||
      typeof req.includeActor !== "boolean" ||
      typeof req.writeHomeTimeline !== "boolean" ||
      !Number.isFinite(requestTime) ||
      requestTime > now + 1000 ||
      requestTime < now - 5 * 60 * 1000;

    if (invalidRequest) {
      await rejectRequest("INVALID DATA");
      return;
    }

    const isTargeted = recipientUid.length > 0;
    const isChatMessage =
      !isTargeted &&
      type === "chat";

    if (isTargeted && !targetedTypes.has(type)) {
      await rejectRequest("TARGET TYPE NOT ALLOWED");
      return;
    }

    if (
      isTargeted &&
      req.writeHomeTimeline !== false
    ) {
      await rejectRequest("TARGET TIMELINE NOT ALLOWED");
      return;
    }

    if (!isTargeted && targetedTypes.has(type)) {
      await rejectRequest("RECIPIENT REQUIRED");
      return;
    }

    const rateKey =
      `${requestedBy}_${homeId}_${recipientUid}_${type}`;

    const lastRequestTime =
      lastHomeNotificationRequestMap[rateKey] || 0;

    const rateLimitMillis =
      isChatMessage ? 150 : 750;

    if (
      now - lastRequestTime <
      rateLimitMillis
    ) {
      await rejectRequest("RATE LIMITED");
      return;
    }

    lastHomeNotificationRequestMap[rateKey] = now;

    const homeSnap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}`)
      .once("value");

    if (!homeSnap.exists()) {
      await rejectRequest("HOME NOT FOUND");
      return;
    }

    const home = homeSnap.val() || {};
    let role = "owner";

    if (
      isTargeted &&
      type === "join_request"
    ) {
      if (recipientUid !== ownerUid) {
        await rejectRequest("INVALID JOIN RECIPIENT");
        return;
      }

      const joinRequestSnap = await db
        .ref(
          `accounts/${ownerUid}/shareRequests/${homeId}_${requestedBy}`,
        )
        .once("value");

      const joinRequest =
        joinRequestSnap.val() || {};

      if (
        joinRequest.type !== "join_request" ||
        joinRequest.ownerUid !== ownerUid ||
        joinRequest.targetUid !== requestedBy ||
        joinRequest.homeId !== homeId
      ) {
        await rejectRequest("JOIN REQUEST NOT FOUND");
        return;
      }

      role = "requester";
    } else if (requestedBy !== ownerUid) {
      const [sharedHomeSnap, sharedMemberSnap] =
        await Promise.all([
          db
            .ref(
              `accounts/${requestedBy}/sharedHomes/${homeId}`,
            )
            .once("value"),

          db
            .ref(
              `sharedByHome/${homeId}/${requestedBy}`,
            )
            .once("value"),
        ]);

      const sharedHome =
        sharedHomeSnap.val() || {};

      if (
        sharedHome.ownerUid !== ownerUid ||
        !sharedMemberSnap.exists()
      ) {
        await rejectRequest("MEMBERSHIP NOT FOUND");
        return;
      }

      role = String(
        sharedHome.role || "",
      ).trim();

      if (
        role !== "admin" &&
        role !== "member"
      ) {
        await rejectRequest("INVALID ROLE");
        return;
      }
    }

    if (
      isTargeted &&
      type === "share_request"
    ) {
      if (
        role !== "owner" &&
        role !== "admin"
      ) {
        await rejectRequest("NO SHARE PERMISSION");
        return;
      }

      const shareRequestSnap = await db
        .ref(
          `accounts/${recipientUid}/shareRequests/${homeId}`,
        )
        .once("value");

      const shareRequest =
        shareRequestSnap.val() || {};

      if (
        shareRequest.type !== "share_request" ||
        shareRequest.ownerUid !== ownerUid ||
        shareRequest.targetUid !== recipientUid ||
        shareRequest.homeId !== homeId
      ) {
        await rejectRequest("SHARE REQUEST NOT FOUND");
        return;
      }
    }

    if (
      isTargeted &&
      type === "transfer_owner_request"
    ) {
      if (
        requestedBy !== ownerUid ||
        recipientUid === ownerUid
      ) {
        await rejectRequest("NO TRANSFER PERMISSION");
        return;
      }

      const transferRequestSnap = await db
        .ref(
          `accounts/${recipientUid}/shareRequests/transfer_${homeId}_${ownerUid}`,
        )
        .once("value");

      const transferRequest =
        transferRequestSnap.val() || {};

      if (
        transferRequest.type !==
          "transfer_owner_request" ||
        transferRequest.homeId !== homeId ||
        transferRequest.oldOwnerUid !== ownerUid ||
        transferRequest.newOwnerUid !== recipientUid
      ) {
        await rejectRequest(
          "TRANSFER REQUEST NOT FOUND",
        );
        return;
      }
    }

    if (
      isTargeted &&
      memberTargetedTypes.has(type)
    ) {
      const recipientAccountSnap = await db
        .ref(`accounts/${recipientUid}`)
        .once("value");

      if (!recipientAccountSnap.exists()) {
        await rejectRequest("RECIPIENT NOT FOUND");
        return;
      }

      let recipientIsMember =
        recipientUid === ownerUid;

      if (!recipientIsMember && type !== "member_removed") {
        const [recipientHomeSnap, recipientMemberSnap] =
          await Promise.all([
            db
              .ref(
                `accounts/${recipientUid}/sharedHomes/${homeId}`,
              )
              .once("value"),

            db
              .ref(
                `sharedByHome/${homeId}/${recipientUid}`,
              )
              .once("value"),
          ]);

        const recipientHome =
          recipientHomeSnap.val() || {};

        recipientIsMember =
          recipientHome.ownerUid === ownerUid &&
          recipientMemberSnap.exists();
      }

      if (
        !recipientIsMember &&
        type !== "member_removed"
      ) {
        await rejectRequest(
          "RECIPIENT MEMBERSHIP NOT FOUND",
        );
        return;
      }

      if (
        (
          type === "join_request_accepted" ||
          type === "member_join"
        ) &&
        role !== "owner" &&
        role !== "admin"
      ) {
        await rejectRequest("NO ACCEPT PERMISSION");
        return;
      }

      if (
        type === "role_changed" &&
        requestedBy !== ownerUid
      ) {
        await rejectRequest("ONLY OWNER CAN CHANGE ROLE");
        return;
      }

      if (
        type === "member_removed" &&
        (
          recipientUid === ownerUid ||
          (
            role !== "owner" &&
            role !== "admin"
          )
        )
      ) {
        await rejectRequest("NO REMOVE PERMISSION");
        return;
      }

      if (
        type === "transfer_owner_accepted" &&
        requestedBy !== ownerUid
      ) {
        await rejectRequest(
          "ONLY NEW OWNER CAN CONFIRM TRANSFER",
        );
        return;
      }
    }

    let verifiedChatMessage = null;
    let verifiedChatMessageId = "";

    if (isChatMessage) {
      const requestData =
        req.data &&
        typeof req.data === "object"
          ? req.data
          : {};

      verifiedChatMessageId =
        String(
          requestData.messageId ||
          entityId ||
          "",
        ).trim();

      const invalidChatRequest =
        category !== "chat" ||
        severity !== "info" ||
        deviceId.length > 0 ||
        entityType !== "chat" ||
        verifiedChatMessageId.length === 0 ||
        verifiedChatMessageId.length > 160 ||
        entityId !== verifiedChatMessageId ||
        req.includeActor !== false ||
        req.writeHomeTimeline !== false;

      if (invalidChatRequest) {
        await rejectRequest(
          "INVALID CHAT REQUEST",
        );
        return;
      }

      const dedupeKey =
        `${homeId}_${verifiedChatMessageId}`;

      if (
        processedChatNotificationMessages.has(
          dedupeKey,
        )
      ) {
        await snap.ref.remove();
        return;
      }

      const chatMessageSnap = await db
        .ref(
          `homeChats/${homeId}/messages/${verifiedChatMessageId}`,
        )
        .once("value");

      if (!chatMessageSnap.exists()) {
        await rejectRequest(
          "CHAT MESSAGE NOT FOUND",
        );
        return;
      }

      verifiedChatMessage =
        chatMessageSnap.val() || {};

      const chatSenderUid =
        String(
          verifiedChatMessage.uid || "",
        ).trim();

      const chatText =
        String(
          verifiedChatMessage.text || "",
        ).trim();

      const chatTime =
        Number(verifiedChatMessage.time);

      if (
        chatSenderUid !== requestedBy ||
        chatText.length === 0 ||
        chatText.length > 1000 ||
        !Number.isFinite(chatTime) ||
        chatTime > now + 1000 ||
        chatTime < now - 5 * 60 * 1000
      ) {
        await rejectRequest(
          "INVALID CHAT MESSAGE",
        );
        return;
      }

      processedChatNotificationMessages.add(
        dedupeKey,
      );

      if (
        processedChatNotificationMessages.size >
        2000
      ) {
        const oldestKey =
          processedChatNotificationMessages
            .values()
            .next()
            .value;

        if (oldestKey) {
          processedChatNotificationMessages.delete(
            oldestKey,
          );
        }
      }
    }

    const isMemberLeave =
      !isTargeted &&
      role === "member" &&
      type === "member_leave";

    if (
      !isTargeted &&
      role === "member" &&
      !isMemberLeave &&
      !isChatMessage
    ) {
      await rejectRequest("MEMBER TYPE NOT ALLOWED");
      return;
    }

    if (
      isMemberLeave &&
      (
        category !== "member" ||
        severity !== "warning" ||
        entityType !== "member" ||
        entityId !== requestedBy ||
        req.includeActor !== false
      )
    ) {
      await rejectRequest("INVALID MEMBER LEAVE");
      return;
    }

    if (
      !isTargeted &&
      category === "device"
    ) {
      const allowsNoDeviceId =
        type === "pair_started";

      if (allowsNoDeviceId) {
        if (
          deviceId.length > 0 ||
          entityType === "device"
        ) {
          await rejectRequest(
            "INVALID DEVICE TARGET",
          );
          return;
        }
      } else if (
        deviceId.length === 0 ||
        !homeSnap
          .child("devices")
          .child(deviceId)
          .exists()
      ) {
        await rejectRequest("DEVICE NOT FOUND");
        return;
      }
    }

    if (
      entityType.length > 0 &&
      entityType !== "home" &&
      entityType !== "device" &&
      entityType !== "member" &&
      entityType !== "chat"
    ) {
      await rejectRequest("INVALID ENTITY TYPE");
      return;
    }

    if (
      !isTargeted &&
      entityType === "device" &&
      (
        deviceId.length === 0 ||
        entityId !== deviceId
      )
    ) {
      await rejectRequest("INVALID DEVICE ENTITY");
      return;
    }

    const actorSnap = await db
      .ref(`accounts/${requestedBy}`)
      .once("value");

    const actor = actorSnap.val() || {};
    const actorProfile = actor.profile || {};

    const actorName =
      String(
        actorProfile.name ||
        actor.name ||
        actor.email ||
        "Một thành viên",
      ).trim() || "Một thành viên";

    const homeName =
      String(home.name || "").trim() || homeId;

    let finalType = type;
    let finalCategory = category;
    let finalSeverity = severity;
    let finalTitle = title;
    let finalMessage = message;
    let finalEntityType =
      entityType || (deviceId ? "device" : "home");
    let finalEntityId =
      entityId || deviceId || homeId;

    if (isMemberLeave) {
      finalCategory = "member";
      finalSeverity = "warning";
      finalTitle = "Thành viên rời nhà";
      finalMessage =
        `${actorName} đã rời khỏi nhà "${homeName}".`;
      finalEntityType = "member";
      finalEntityId = requestedBy;
    }

    if (
      isTargeted &&
      type === "share_request"
    ) {
      finalCategory = "member";
      finalSeverity = "info";
      finalTitle = "Lời mời chia sẻ nhà";
      finalMessage =
        `${actorName} đã mời bạn tham gia nhà "${homeName}".`;
      finalEntityType = "home";
      finalEntityId = homeId;
    }

    if (
      isTargeted &&
      type === "join_request"
    ) {
      finalCategory = "member";
      finalSeverity = "info";
      finalTitle = "Yêu cầu gia nhập nhà";
      finalMessage =
        `${actorName} đang xin gia nhập nhà "${homeName}".`;
      finalEntityType = "member";
      finalEntityId = requestedBy;
    }

    if (
      isTargeted &&
      type === "transfer_owner_request"
    ) {
      finalCategory = "member";
      finalSeverity = "info";
      finalTitle = "Yêu cầu chuyển quyền chủ nhà";
      finalMessage =
        `${actorName} muốn chuyển quyền chủ nhà "${homeName}" cho bạn.`;
      finalEntityType = "home";
      finalEntityId = homeId;
    }

    if (
      isChatMessage &&
      verifiedChatMessage
    ) {
      const verifiedText =
        String(
          verifiedChatMessage.text || "",
        ).trim();

      finalType = "chat";
      finalCategory = "chat";
      finalSeverity = "info";
      finalTitle = homeName;
      finalMessage =
        `${actorName}: ${verifiedText}`;
      finalEntityType = "chat";
      finalEntityId =
        verifiedChatMessageId;
    }

    const recipientUids = new Set();

    if (isTargeted) {
      recipientUids.add(recipientUid);
    } else {
      recipientUids.add(ownerUid);

      const sharedSnap = await db
        .ref(`sharedByHome/${homeId}`)
        .once("value");

      const sharedUsers =
        sharedSnap.val() || {};

      for (const sharedUid of Object.keys(sharedUsers)) {
        const membershipSnap = await db
          .ref(
            `accounts/${sharedUid}/sharedHomes/${homeId}`,
          )
          .once("value");

        const membership =
          membershipSnap.val() || {};

        if (membership.ownerUid === ownerUid) {
          recipientUids.add(sharedUid);
        }
      }

      if (
        isMemberLeave ||
        req.includeActor !== true
      ) {
        recipientUids.delete(requestedBy);
      }
    }

    for (const targetUid of recipientUids) {
      if (
        isChatMessage &&
        verifiedChatMessage
      ) {
        const unreadCount =
          await countUnreadChatMessages(
            homeId,
            targetUid,
          );

        await sendChatNotificationPush({
          receiverUid: targetUid,
          ownerUid,
          homeId,
          homeName,
          senderUid: requestedBy,
          senderName: actorName,
          messageId:
            verifiedChatMessageId,
          text:
            String(
              verifiedChatMessage.text || "",
            ).trim(),
          unreadCount,
        });

        continue;
      }

      await addHomeNotificationFromBackend({
        uid: targetUid,
        homeId,
        homeName,
        type: finalType,
        category: finalCategory,
        severity: finalSeverity,
        title: finalTitle,
        message: finalMessage,
        entityType: finalEntityType,
        entityId: finalEntityId,
      });
    }

    if (
      !isTargeted &&
      !isChatMessage &&
      req.writeHomeTimeline === true
    ) {
      const eventsRef = db.ref(
        `accounts/${ownerUid}/homes/${homeId}/events`,
      );

      const eventRef = eventsRef.push();

      await eventRef.set({
        time: Date.now(),
        text: finalMessage,
        type: finalType,
        senderUid: requestedBy,
        senderName: actorName,
        senderRole: role,
        deviceId: deviceId || "",
        deviceName:
          deviceId &&
          home.devices &&
          home.devices[deviceId]
            ? String(
                home.devices[deviceId].name ||
                deviceId,
              )
            : "",
      });

      const eventsSnap = await eventsRef
        .orderByChild("time")
        .once("value");

      const events =
        eventsSnap.val() || {};

      const entries =
        Object.entries(events);

      if (entries.length > 200) {
        entries.sort((a, b) => {
          return (
            Number(a[1]?.time || 0) -
            Number(b[1]?.time || 0)
          );
        });

        const updates = {};
        const removeCount =
          entries.length - 200;

        for (
          const [eventId] of entries.slice(
            0,
            removeCount,
          )
        ) {
          updates[eventId] = null;
        }

        await eventsRef.update(updates);
      }
    }

    await snap.ref.remove();

    console.log(
      "✅ HOME NOTIFICATION REQUEST APPLIED:",
      requestId,
      requestedBy,
      role,
      finalType,
      recipientUids.size,
    );
  } catch (err) {
    console.log(
      "HOME NOTIFICATION REQUEST ERROR:",
      requestId,
      err.message,
    );

    try {
      await snap.ref.remove();
    } catch (_) {}
  } finally {
    if (requestId) {
      homeNotificationRequestInProgress.delete(
        requestId,
      );
    }
  }
});
const transferOwnerAcceptInProgress = new Set();

function normalizeHomeOrder(rawOrder) {
  if (Array.isArray(rawOrder)) {
    return rawOrder
      .filter((value) => value != null)
      .map((value) => String(value))
      .filter((value) => value.length > 0);
  }

  if (
    rawOrder &&
    typeof rawOrder === "object"
  ) {
    return Object.keys(rawOrder)
      .sort((a, b) => Number(a) - Number(b))
      .map((key) => rawOrder[key])
      .filter((value) => value != null)
      .map((value) => String(value))
      .filter((value) => value.length > 0);
  }

  return [];
}

db.ref("transfer_owner_accept_requests").on(
  "child_added",
  async (snap) => {
    const req = snap.val();
    const requestId = snap.key;

    async function finishRequest(
      status,
      errorMessage = "",
    ) {
      try {
        const result = {
          status,
          processedAt: Date.now(),
        };

        if (errorMessage) {
          result.error = errorMessage;
        }

        await snap.ref.update(result);

        setTimeout(async () => {
          try {
            await snap.ref.remove();
          } catch (_) {}
        }, 30000);
      } catch (_) {}
    }

    try {
      if (!req || !requestId) {
        return;
      }

      if (
        transferOwnerAcceptInProgress.has(
          requestId,
        )
      ) {
        return;
      }

      transferOwnerAcceptInProgress.add(
        requestId,
      );

      const requestedByUid = String(
        req.requestedByUid || "",
      ).trim();

      const oldOwnerUid = String(
        req.oldOwnerUid || "",
      ).trim();

      const newOwnerUid = String(
        req.newOwnerUid || "",
      ).trim();

      const homeId = String(
        req.homeId || "",
      ).trim();

      const requestTime = Number(req.time);
      const now = Date.now();

      const invalidRequest =
        req.status !== "pending" ||
        requestedByUid.length === 0 ||
        oldOwnerUid.length === 0 ||
        newOwnerUid.length === 0 ||
        homeId.length === 0 ||
        requestedByUid !== newOwnerUid ||
        oldOwnerUid === newOwnerUid ||
        !Number.isFinite(requestTime) ||
        requestTime > now + 1000 ||
        requestTime < now - 5 * 60 * 1000;

      if (invalidRequest) {
        await finishRequest(
          "rejected",
          "INVALID DATA",
        );
        return;
      }

      const transferRequestKey =
        `transfer_${homeId}_${oldOwnerUid}`;

      const [
        oldHomeSnap,
        targetHomeSnap,
        transferRequestSnap,
        sharedByHomeSnap,
        oldShareListSnap,
        oldOwnerDirectorySnap,
        newOwnerAccountSnap,
        newOwnerOrderSnap,
      ] = await Promise.all([
        db
          .ref(
            `accounts/${oldOwnerUid}/homes/${homeId}`,
          )
          .once("value"),

        db
          .ref(
            `accounts/${newOwnerUid}/homes/${homeId}`,
          )
          .once("value"),

        db
          .ref(
            `accounts/${newOwnerUid}/shareRequests/${transferRequestKey}`,
          )
          .once("value"),

        db
          .ref(`sharedByHome/${homeId}`)
          .once("value"),

        db
          .ref(
            `accounts/${oldOwnerUid}/shareList/${homeId}`,
          )
          .once("value"),

        db
          .ref(
            `userDirectory/${oldOwnerUid}`,
          )
          .once("value"),

        db
          .ref(`accounts/${newOwnerUid}`)
          .once("value"),

        db
          .ref(
            `accounts/${newOwnerUid}/homeOrder`,
          )
          .once("value"),
      ]);

      if (
        !oldHomeSnap.exists() ||
        !newOwnerAccountSnap.exists()
      ) {
        await finishRequest(
          "rejected",
          "ACCOUNT OR HOME NOT FOUND",
        );
        return;
      }

      if (targetHomeSnap.exists()) {
        await finishRequest(
          "rejected",
          "TARGET HOME ALREADY EXISTS",
        );
        return;
      }

      const transferRequest =
        transferRequestSnap.val() || {};

      const validTransferRequest =
        transferRequest.type ===
          "transfer_owner_request" &&
        transferRequest.homeId === homeId &&
        transferRequest.oldOwnerUid ===
          oldOwnerUid &&
        transferRequest.newOwnerUid ===
          newOwnerUid;

      if (!validTransferRequest) {
        await finishRequest(
          "rejected",
          "TRANSFER REQUEST NOT FOUND",
        );
        return;
      }

      const homeData =
        oldHomeSnap.val() || {};

      const storedOwnerUid = String(
        homeData._ownerUid || "",
      ).trim();

      if (storedOwnerUid !== oldOwnerUid) {
        await finishRequest(
          "rejected",
          "OWNER MISMATCH",
        );
        return;
      }

      const migratedHome = {
        ...homeData,
        _ownerUid: newOwnerUid,
        _shared: false,
      };

      const sharedByHome =
        sharedByHomeSnap.val() || {};

      const oldShareList =
        oldShareListSnap.val() || {};

      const oldOwnerDirectory =
        oldOwnerDirectorySnap.val() || {};

      const oldOwnerMemberData = {
        role: "member",
        email: String(
          oldOwnerDirectory.email || "",
        ),
        name: String(
          oldOwnerDirectory.name || "",
        ),
        photoUrl: String(
          oldOwnerDirectory.photoUrl || "",
        ),
        sharedAt: Date.now(),
      };

      const oldOwnerSharedHome = {
        ownerUid: newOwnerUid,
        role: "member",
      };

      if (homeData.alarmPauseToday) {
        oldOwnerSharedHome.alarmPauseToday =
          homeData.alarmPauseToday;
      }

      const newShareList = {};

      for (
        const [memberUid, rawMember]
        of Object.entries(sharedByHome)
      ) {
        if (
          memberUid === newOwnerUid ||
          memberUid === oldOwnerUid
        ) {
          continue;
        }

        const memberData =
          rawMember &&
          typeof rawMember === "object"
            ? rawMember
            : {};

        const oldListData =
          oldShareList[memberUid] &&
          typeof oldShareList[memberUid] ===
            "object"
            ? oldShareList[memberUid]
            : {};

        newShareList[memberUid] = {
          ...memberData,
          ...oldListData,
          role:
            memberData.role ||
            oldListData.role ||
            "member",
        };
      }

      newShareList[oldOwnerUid] = {
        ...oldOwnerMemberData,
      };

      const newOwnerOrder =
        normalizeHomeOrder(
          newOwnerOrderSnap.val(),
        );

      if (!newOwnerOrder.includes(homeId)) {
        newOwnerOrder.push(homeId);
      }

      const updates = {
        [`accounts/${newOwnerUid}/homes/${homeId}`]:
          migratedHome,

        [`accounts/${oldOwnerUid}/homes/${homeId}`]:
          null,

        [`accounts/${newOwnerUid}/sharedHomes/${homeId}`]:
          null,

        [`accounts/${oldOwnerUid}/sharedHomes/${homeId}`]:
          oldOwnerSharedHome,

        [`sharedByHome/${homeId}/${newOwnerUid}`]:
          null,

        [`sharedByHome/${homeId}/${oldOwnerUid}`]:
          oldOwnerMemberData,

        [`accounts/${oldOwnerUid}/shareList/${homeId}`]:
          null,

        [`accounts/${newOwnerUid}/shareList/${homeId}`]:
          newShareList,

        [`accounts/${newOwnerUid}/homeOrder`]:
          newOwnerOrder,

        [`accounts/${newOwnerUid}/customRules/${homeId}`]:
          null,
      };

      for (
        const memberUid
        of Object.keys(sharedByHome)
      ) {
        if (
          memberUid === newOwnerUid ||
          memberUid === oldOwnerUid
        ) {
          continue;
        }

        updates[
          `accounts/${memberUid}/sharedHomes/${homeId}/ownerUid`
        ] = newOwnerUid;
      }

      const devices =
        homeData.devices &&
        typeof homeData.devices === "object"
          ? homeData.devices
          : {};

      for (
        const deviceId
        of Object.keys(devices)
      ) {
        updates[
          `system/devices_by_ieee/${deviceId}/uid`
        ] = newOwnerUid;

        updates[
          `system/devices_by_ieee/${deviceId}/homeId`
        ] = homeId;
      }

      await db.ref().update(updates);

      for (
        const deviceId
        of Object.keys(devices)
      ) {
        deviceMap[deviceId] = {
          uid: newOwnerUid,
          homeId,
        };
      }

      await finishRequest("completed");

      console.log(
        "👑 TRANSFER OWNER COMPLETED:",
        oldOwnerUid,
        "→",
        newOwnerUid,
        homeId,
      );
    } catch (err) {
      console.log(
        "TRANSFER OWNER ACCEPT ERROR:",
        requestId,
        err.message,
      );

      await finishRequest(
        "rejected",
        err.message || "UNKNOWN ERROR",
      );
    } finally {
      if (requestId) {
        transferOwnerAcceptInProgress.delete(
          requestId,
        );
      }
    }
  },
);
db.ref("alarm_pause_requests").on("child_added", async (snap) => {
  const req = snap.val();
  const requestId = snap.key;

  async function reject(reason) {
    console.log(
      "❌ ALARM PAUSE REQUEST REJECTED:",
      requestId,
      reason,
    );

    try {
      await snap.ref.remove();
    } catch (_) { }
  }

  try {
    if (!req || !requestId) {
      return;
    }

    const ownerUid = String(
      req.ownerUid || "",
    ).trim();

    const homeId = String(
      req.homeId || "",
    ).trim();

    const createdByUid = String(
      req.createdByUid || "",
    ).trim();

    const action = String(
      req.action || "create",
    ).trim();

    const createdAt = Number(req.createdAt);
    const now = Date.now();

    if (
      req.status !== "pending" ||
      ownerUid.length === 0 ||
      homeId.length === 0 ||
      createdByUid.length === 0 ||
      !requestId.endsWith(`_${createdByUid}`) ||
      !Number.isFinite(createdAt) ||
      createdAt > now + 1000 ||
      createdAt < now - 5 * 60 * 1000 ||
      (action !== "create" && action !== "remove")
    ) {
      await reject("INVALID DATA");
      return;
    }

    const homeSnap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}`)
      .once("value");

    if (!homeSnap.exists()) {
      await reject("HOME NOT FOUND");
      return;
    }

    const home = homeSnap.val() || {};

    let hasPermission = createdByUid === ownerUid;

    if (!hasPermission) {
      const [sharedHomeSnap, sharedMemberSnap] =
        await Promise.all([
          db
            .ref(
              `accounts/${createdByUid}/sharedHomes/${homeId}`,
            )
            .once("value"),

          db
            .ref(
              `sharedByHome/${homeId}/${createdByUid}`,
            )
            .once("value"),
        ]);

      const sharedHome = sharedHomeSnap.val() || {};

      hasPermission =
        sharedHome.ownerUid === ownerUid &&
        sharedMemberSnap.exists();
    }

    if (!hasPermission) {
      await reject("NO PERMISSION");
      return;
    }

    const actorSnap = await db
      .ref(`accounts/${createdByUid}`)
      .once("value");

    const actor = actorSnap.val() || {};
    const actorProfile = actor.profile || {};

    const trustedActorName =
      String(
        actorProfile.name ||
        actor.name ||
        actor.email ||
        "Một thành viên",
      ).trim() || "Một thành viên";

    const sharedSnap = await db
      .ref(`sharedByHome/${homeId}`)
      .once("value");

    const sharedUsers = sharedSnap.val() || {};

    if (action === "remove") {
      const updates = {
        [`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`]:
          null,

        [`alarm_pause_requests/${requestId}`]:
          null,
      };

      for (const sharedUid of Object.keys(sharedUsers)) {
        if (sharedUid === ownerUid) {
          continue;
        }

        updates[
          `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`
        ] = null;
      }

      await db.ref().update(updates);

      console.log(
        "🧹 ALARM PAUSE REMOVED:",
        ownerUid,
        homeId,
        createdByUid,
      );

      return;
    }

    const date = String(req.date || "").trim();
    const start = String(req.start || "").trim();
    const end = String(req.end || "").trim();
    const reason = String(req.reason || "").trim();

    if (
      date !== getTodayKey() ||
      !isValidHHMM(start) ||
      !isValidHHMM(end) ||
      start === end ||
      reason.length > 120
    ) {
      await reject("INVALID PAUSE DATA");
      return;
    }

    const devices = home.devices || {};

    const enabledAlarms = Object.values(devices)
      .map((device) => device?.alarm)
      .filter((alarm) => {
        return (
          alarm &&
          alarm.enabled === true &&
          isValidHHMM(alarm.start) &&
          isValidHHMM(alarm.end)
        );
      });

    const insideAlarmRange = enabledAlarms.some((alarm) => {
      return isPauseInsideAlarm(
        alarm.start,
        alarm.end,
        start,
        end,
      );
    });

    if (!insideAlarmRange) {
      await reject("OUTSIDE ALARM RANGE");
      return;
    }

    const trustedHomeName =
      String(home.name || "").trim() || homeId;

    const pauseData = {
      date,
      start,
      end,
      homeName: trustedHomeName,
      reason,
      createdByUid,
      createdByName: trustedActorName,
      createdAt: Date.now(),
    };

    const updates = {
      [`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`]:
        pauseData,

      [`alarm_pause_requests/${requestId}`]:
        null,
    };

    for (const sharedUid of Object.keys(sharedUsers)) {
      if (sharedUid === ownerUid) {
        continue;
      }

      updates[
        `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`
      ] = pauseData;
    }

    await db.ref().update(updates);

    console.log(
      "⏸️ ALARM PAUSE REQUEST APPLIED:",
      ownerUid,
      homeId,
      createdByUid,
    );
  } catch (err) {
    console.log(
      "ALARM PAUSE REQUEST ERROR:",
      err.message,
    );

    try {
      await snap.ref.remove();
    } catch (_) { }
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

// ================= ALARM INCIDENT ACTION REQUEST =================
db.ref("alarm_incident_action_requests").on(
  "child_added",
  async (snap) => {
    const req = snap.val();
    const requestId = snap.key;

    async function reject(reason) {
      console.log(
        "❌ ALARM INCIDENT ACTION REJECTED:",
        requestId,
        reason,
      );

      try {
        await snap.ref.remove();
      } catch (_) { }
    }

    try {
      if (!req || !requestId) {
        return;
      }

      if (alarmIncidentActionInProgress.has(requestId)) {
        return;
      }

      alarmIncidentActionInProgress.add(requestId);

      const receiverUid = String(
        req.receiverUid || "",
      ).trim();
      const incidentId = String(
        req.incidentId || "",
      ).trim();
      const requestedBy = String(
        req.requestedBy || "",
      ).trim();
      const action = String(
        req.action || "",
      ).trim();
      const createdAt = Number(req.createdAt);
      const now = Date.now();

      const allowedActions = new Set([
        "stop",
        "check_home",
        "resolve",
      ]);

      if (
        req.status !== "pending" ||
        !receiverUid ||
        !incidentId ||
        !requestedBy ||
        !allowedActions.has(action) ||
        !Number.isFinite(createdAt) ||
        createdAt > now + 1000 ||
        createdAt < now - 5 * 60 * 1000
      ) {
        await reject("INVALID DATA");
        return;
      }

      const incidentSnap = await db
        .ref(
          `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
        )
        .once("value");

      const incident = incidentSnap.val();

      if (!incident || incident.status !== "active") {
        await reject("INCIDENT NOT ACTIVE");
        return;
      }

      const ownerUid = String(
        incident.ownerUid || "",
      ).trim();
      const homeId = String(
        incident.homeId || "",
      ).trim();

      if (!ownerUid || !homeId) {
        await reject("INVALID INCIDENT");
        return;
      }

      let hasPermission = requestedBy === ownerUid;

      if (!hasPermission) {
        const [sharedHomeSnap, sharedMemberSnap] =
          await Promise.all([
            db
              .ref(
                `accounts/${requestedBy}/sharedHomes/${homeId}`,
              )
              .once("value"),
            db
              .ref(
                `sharedByHome/${homeId}/${requestedBy}`,
              )
              .once("value"),
          ]);

        const sharedHome = sharedHomeSnap.val() || {};

        hasPermission =
          sharedHome.ownerUid === ownerUid &&
          sharedMemberSnap.exists();
      }

      if (!hasPermission) {
        await reject("NO PERMISSION");
        return;
      }

      await resolveAlarmIncidentsForHome({
        ownerUid,
        homeId,
        resolvedBy: requestedBy,
        action,
      });

      await snap.ref.remove();
    } catch (err) {
      console.log(
        "ALARM INCIDENT ACTION ERROR:",
        requestId,
        err.message,
      );

      try {
        await snap.ref.remove();
      } catch (_) { }
    } finally {
      if (requestId) {
        alarmIncidentActionInProgress.delete(requestId);
      }
    }
  },
);

init();

// ================= MQTT CONNECT =================
client.on("connect", () => {
  mqttConnected = true;

  console.log("MQTT CONNECTED");
  client.subscribe("zigbee2mqtt/#");

  // Cập nhật ngay để app biết MQTT đã hoạt động.
  void writeHubHeartbeat();
});

client.on("offline", () => {
  mqttConnected = false;
  void writeHubHeartbeat();
});

client.on("close", () => {
  mqttConnected = false;
});

// ================= PAIRING =================
db.ref("pair_requests").on("child_added", async (snap) => {
  const data = snap.val();
  const key = snap.key;

  if (!data || !key) return;

  const rawDuration = Number(data.duration);

  const cleanupSeconds =
    Number.isFinite(rawDuration) &&
      rawDuration >= 1 &&
      rawDuration <= 60
      ? rawDuration
      : 60;

  setTimeout(async () => {
    try {
      if (pairingSession?.key === key) {
        await setPermitJoin(false);
        pairingSession = null;
      }

      await snap.ref.remove();

      console.log("🧹 PAIR REQUEST REMOVED:", key);
    } catch (err) {
      console.log(
        "PAIR REQUEST CLEANUP ERROR:",
        err.message,
      );
    }
  }, cleanupSeconds * 1000);

  try {
    const requestedBy = String(
      data.requestedBy || "",
    ).trim();

    const ownerUid = String(
      data.ownerUid || "",
    ).trim();

    const homeId = String(
      data.homeId || "",
    ).trim();

    const hubId = String(
      data.hubId || "",
    ).trim();

    const roomId = String(
      data.roomId || "unassigned",
    ).trim();

    const duration = Number(data.duration);
    const requestTime = Number(data.time);
    const now = Date.now();

    const invalidRequest =
      data.active !== true ||
      requestedBy.length === 0 ||
      ownerUid.length === 0 ||
      homeId.length === 0 ||
      hubId.length === 0 ||
      roomId.length === 0 ||
      !Number.isFinite(duration) ||
      duration < 1 ||
      duration > 60 ||
      !Number.isFinite(requestTime) ||
      requestTime > now + 1000 ||
      requestTime < now - 5 * 60 * 1000;

    if (invalidRequest) {
      console.log(
        "❌ PAIR REQUEST REJECTED: INVALID DATA",
        key,
      );

      await snap.ref.remove();
      return;
    }

    // Request dành cho Raspberry Pi khác.
    if (hubId !== DEVICE_ID.trim()) {
      return;
    }

    const homeSnap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}`)
      .once("value");

    if (!homeSnap.exists()) {
      console.log(
        "❌ PAIR REQUEST REJECTED: HOME NOT FOUND",
        key,
      );

      await snap.ref.remove();
      return;
    }

    let hasPermission = requestedBy === ownerUid;

    if (!hasPermission) {
      const sharedSnap = await db
        .ref(
          `accounts/${requestedBy}/sharedHomes/${homeId}`,
        )
        .once("value");

      const sharedInfo = sharedSnap.val() || {};

      hasPermission =
        sharedInfo.ownerUid === ownerUid &&
        sharedInfo.role === "admin";
    }

    if (!hasPermission) {
      console.log(
        "❌ PAIR REQUEST REJECTED: NO PERMISSION",
        key,
        requestedBy,
      );

      await snap.ref.remove();
      return;
    }

    if (
      roomId !== "unassigned" &&
      !homeSnap
        .child("rooms")
        .child(roomId)
        .exists()
    ) {
      console.log(
        "❌ PAIR REQUEST REJECTED: ROOM NOT FOUND",
        key,
        roomId,
      );

      await snap.ref.remove();
      return;
    }

    if (pairingSession?.key === key) {
      return;
    }

    if (pairingSession != null) {
      console.log(
        "❌ PAIR REQUEST REJECTED: HUB BUSY",
        key,
      );

      await snap.ref.remove();
      return;
    }

    await setPermitJoin(true, duration);

    pairingSession = {
      key,
      uid: ownerUid,
      requestedBy,
      homeId,
      roomId,
    };

    console.log(
      "🟢 PAIR START:",
      key,
      homeId,
      requestedBy,
    );
  } catch (err) {
    console.log(
      "PAIR REQUEST PROCESS ERROR:",
      err.message,
    );

    if (pairingSession?.key === key) {
      try {
        await setPermitJoin(false);
      } catch (_) { }

      pairingSession = null;
    }

    try {
      await snap.ref.remove();
    } catch (_) { }
  }
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

          case "window":
            defaultName = `Cửa sổ ${sameTypeCount}`;
            break;

          case "gate":
            defaultName = `Cổng ${sameTypeCount}`;
            break;

          case "door_lock":
          case "lock":
            defaultName = `Khóa thông minh ${sameTypeCount}`;
            break;

          case "motion":
            defaultName = `Cảm biến chuyển động ${sameTypeCount}`;
            break;

          case "presence":
            defaultName = `Cảm biến hiện diện ${sameTypeCount}`;
            break;

          case "vibration":
            defaultName = `Cảm biến rung ${sameTypeCount}`;
            break;

          case "glass_break":
            defaultName = `Cảm biến kính vỡ ${sameTypeCount}`;
            break;

          case "smoke":
            defaultName = `Báo cháy ${sameTypeCount}`;
            break;

          case "heat":
            defaultName = `Cảm biến nhiệt nguy hiểm ${sameTypeCount}`;
            break;

          case "carbon_monoxide":
            defaultName = `Cảm biến khí CO ${sameTypeCount}`;
            break;

          case "gas":
            defaultName = `Cảm biến gas ${sameTypeCount}`;
            break;

          case "water_leak":
          case "flood":
            defaultName = `Cảm biến ngập nước ${sameTypeCount}`;
            break;

          case "temperature":
            defaultName = `Nhiệt độ ${sameTypeCount}`;
            break;

          case "sos":
            defaultName = `SOS ${sameTypeCount}`;
            break;

          case "smart_plug":
            defaultName = `Ổ điện thông minh ${sameTypeCount}`;
            break;

          case "power_monitor":
            defaultName = `Đo điện năng ${sameTypeCount}`;
            break;

          case "ups":
            defaultName = `Nguồn dự phòng ${sameTypeCount}`;
            break;

          case "siren":
            defaultName = `Còi báo động ${sameTypeCount}`;
            break;

          case "smart_valve":
            defaultName = `Van thông minh ${sameTypeCount}`;
            break;

          case "camera":
            defaultName = `Camera ${sameTypeCount}`;
            break;

          case "doorbell":
            defaultName = `Chuông cửa ${sameTypeCount}`;
            break;

          case "keypad":
            defaultName = `Bàn phím an ninh ${sameTypeCount}`;
            break;

          case "repeater":
            defaultName = `Bộ mở rộng sóng ${sameTypeCount}`;
            break;

          default:
            defaultName = `Thiết bị chưa nhận diện ${sameTypeCount}`;
        }
        await db.ref(`accounts/${uid}/homes/${homeId}/devices/${ieee}`).set({
          name: defaultName,
          ieee,
          type: deviceType,
          roomId: roomId || "unassigned",
          alarm:
            isSecurityDeviceType(deviceType)
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

    const currentDeviceType = String(
      oldData.type || "unknown",
    ).trim();

    const inferredDeviceType =
      inferDeviceTypeFromPayload(
        data,
        currentDeviceType,
      );

    let updateData = {
      updated_at: now,
    };

    if (
      currentDeviceType === "unknown" &&
      inferredDeviceType !== "unknown"
    ) {
      updateData.type = inferredDeviceType;
    }

    const fieldsToCopy = [
      "availability",
      "last_seen",
      "linkquality",
      "contact",
      "tamper",
      "battery",
      "battery_low",
      "smoke",
      "temperature",
      "humidity",
      "action",
      "occupancy",
      "motion",
      "presence",
      "vibration",
      "vibration_strength",
      "angle",
      "x_axis",
      "y_axis",
      "z_axis",
      "gas",
      "gas_alarm",
      "carbon_monoxide",
      "co_alarm",
      "water_leak",
      "leak",
      "water",
      "heat",
      "heat_alarm",
      "high_temperature_alarm",
      "lock",
      "lock_state",
      "state",
      "power",
      "current",
      "voltage",
      "energy",
      "consumption",
      "device_temperature",
      "switch_type",
    ];

    for (const field of fieldsToCopy) {
      if (data[field] !== undefined) {
        updateData[field] = data[field];
      }
    }

    const eventFields = [
      "contact",
      "tamper",
      "smoke",
      "action",
      "occupancy",
      "motion",
      "presence",
      "vibration",
      "gas",
      "gas_alarm",
      "carbon_monoxide",
      "co_alarm",
      "water_leak",
      "leak",
      "water",
      "heat",
      "heat_alarm",
      "high_temperature_alarm",
      "lock",
      "lock_state",
      "state",
    ];

    if (
      eventFields.some((field) => {
        return (
          data[field] !== undefined &&
          data[field] !== oldData[field]
        );
      })
    ) {
      updateData.last_event = now;
    }

    if (data.battery !== undefined) {
      updateData.battery_status = "percent";
    }

    if (data.battery_low !== undefined) {
      updateData.battery_status =
        data.battery_low === true ? "low" : "ok";
    }

    const resolvedDeviceType =
      updateData.type ||
      currentDeviceType ||
      inferredDeviceType ||
      "unknown";

    if (
      resolvedDeviceType === "sos" &&
      data.action !== undefined
    ) {
      updateData.last_triggered = now;
      updateData.sos_active_until =
        now + 5 * 60 * 1000;
    }


    await deviceRef.update(updateData);


    if (updateData.last_event !== undefined) {
      let statusText = "";

      const currentType = updateData.type || oldData.type || "door";

      if (
        currentType === "door" ||
        currentType === "window" ||
        currentType === "gate"
      ) {
        statusText =
          updateData.contact === false
            ? "Cửa mở"
            : "Cửa đóng";
      } else if (
        currentType === "door_lock" ||
        currentType === "lock"
      ) {
        statusText =
          normalizeLockState({
            ...oldData,
            ...updateData,
          }) === "unlocked"
            ? "Khóa đã mở"
            : "Khóa đã đóng";
      } else if (
        currentType === "motion" ||
        currentType === "presence"
      ) {
        statusText = "Phát hiện chuyển động";
      } else if (currentType === "vibration") {
        statusText = "Phát hiện rung/chấn động";
      } else if (currentType === "glass_break") {
        statusText = "Phát hiện kính vỡ";
      } else if (currentType === "smoke") {
        statusText = isActiveSignal(updateData.smoke)
          ? "Phát hiện khói"
          : "Khói đã trở lại bình thường";
      } else if (currentType === "heat") {
        statusText = "Cập nhật cảnh báo nhiệt";
      } else if (currentType === "carbon_monoxide") {
        statusText = "Cập nhật cảm biến khí CO";
      } else if (currentType === "gas") {
        statusText = "Cập nhật cảm biến gas";
      } else if (
        currentType === "water_leak" ||
        currentType === "flood"
      ) {
        statusText = "Cập nhật cảm biến ngập nước";
      } else if (currentType === "sos") {
        statusText = "Nút SOS đã được bấm";
      } else if (currentType === "temperature") {
        statusText = "Cập nhật nhiệt độ / độ ẩm";
      } else if (currentType === "smart_plug") {
        statusText = "Ổ điện thông minh đã cập nhật";
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
        uid,
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