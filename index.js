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
let chatUnreadMigrationPromise = null;

// ================= LIMITED TIMELINES =================
// Giới hạn lưu thực tế. UI vẫn chỉ tải số lượng cần hiển thị.
const HOME_NOTIFICATION_STORAGE_LIMIT = 120;
const DEVICE_NOTIFICATION_STORAGE_LIMIT = 100;
const HOME_EVENT_STORAGE_LIMIT = 200;
const ORDERED_LIST_CLEANUP_BATCH_SIZE = 20;
const ORDERED_LIST_CLEANUP_MAX_PASSES = 20;

const orderedListCleanupTimerMap = {};
const orderedListCleanupInProgress = new Set();

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

// Không polling dày. Watchdog chỉ chạy mỗi 60 giây và dùng cache
// để dọn incident bị bỏ sót khi listener hoặc backend vừa khởi động lại.
const ALARM_INCIDENT_WATCHDOG_INTERVAL_MS = 60 * 1000;

// Khi gửi push Alarm thất bại, không được đánh dấu đã sang cấp mới.
// Thử lại có giới hạn để tránh spam khi thiết bị không còn FCM token.
const ALARM_STAGE_RETRY_DELAY_MS = 15 * 1000;
const ALARM_STAGE_MAX_RETRY_COUNT = 4;

// Hub ghi heartbeat lên Firebase mỗi 30 giây.
// App sẽ coi hub Offline khi lastHeartbeatAt quá 90 giây.
const HUB_HEARTBEAT_INTERVAL_MS = 60 * 1000;
const HUB_HEARTBEAT_STARTED_AT = Date.now();

const alarmIncidentTimerMap = {};
const alarmIncidentAdvanceInProgress = new Set();
const alarmIncidentActionInProgress = new Set();
const alarmIncidentValidationPromiseMap = new Map();
const alarmIncidentStartPromiseMap = new Map();
const alarmIncidentQueuedStageMap = new Map();
const alarmIncidentStageRetryCountMap = new Map();
const localActiveAlarmIncidentMap = new Map();
let alarmIncidentWatchdogTimer = null;
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

async function trimOrderedListByTime(
  listRef,
  maxItems,
) {
  if (!listRef || maxItems <= 0) {
    return;
  }

  const queryLimit =
    maxItems + ORDERED_LIST_CLEANUP_BATCH_SIZE;

  for (
    let pass = 0;
    pass < ORDERED_LIST_CLEANUP_MAX_PASSES;
    pass++
  ) {
    const snap = await listRef
      .orderByChild("time")
      .limitToFirst(queryLimit)
      .once("value");

    const orderedKeys = [];

    snap.forEach((childSnap) => {
      if (childSnap.key) {
        orderedKeys.push(childSnap.key);
      }
    });

    if (orderedKeys.length <= maxItems) {
      return;
    }

    const removeCount =
      orderedKeys.length - maxItems;
    const updates = {};

    for (const key of orderedKeys.slice(0, removeCount)) {
      updates[key] = null;
    }

    await listRef.update(updates);

    // Nếu query trả về ít hơn giới hạn truy vấn thì danh sách đã
    // được đưa về đúng maxItems trong lần này.
    if (orderedKeys.length < queryLimit) {
      return;
    }
  }
}

function queueOrderedListCleanup(
  cleanupKey,
  listRef,
  maxItems,
) {
  if (!cleanupKey || !listRef || maxItems <= 0) {
    return;
  }

  if (orderedListCleanupTimerMap[cleanupKey]) {
    return;
  }

  orderedListCleanupTimerMap[cleanupKey] = setTimeout(
    async () => {
      delete orderedListCleanupTimerMap[cleanupKey];

      if (orderedListCleanupInProgress.has(cleanupKey)) {
        // Có ghi mới trong lúc cleanup đang chạy: xếp thêm một lượt
        // ngắn sau đó thay vì chạy song song.
        queueOrderedListCleanup(
          cleanupKey,
          listRef,
          maxItems,
        );
        return;
      }

      orderedListCleanupInProgress.add(cleanupKey);

      try {
        await trimOrderedListByTime(
          listRef,
          maxItems,
        );
      } catch (error) {
        console.log(
          "ORDERED LIST CLEANUP ERROR:",
          cleanupKey,
          error.message,
        );
      } finally {
        orderedListCleanupInProgress.delete(cleanupKey);
      }
    },
    1500,
  );
}

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
      hubHeartbeatWriteCount % 10 === 0
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
// ================= BACKEND DATA CACHE =================
// Các tác vụ lặp dùng cache theo child thay vì tải lại toàn bộ /accounts.
// Chỉ có một lần bootstrap khi backend khởi động; sau đó cache được cập nhật
// bằng child_added / child_changed / child_removed.
const accountCache = new Map();
const sharedByHomeCache = new Map();
let backendDataCacheStarted = false;

function getCachedAccountsObject() {
  return Object.fromEntries(accountCache.entries());
}

function getCachedSharedByHomeObject() {
  return Object.fromEntries(sharedByHomeCache.entries());
}

function buildUserDirectoryData(rawUser) {
  const user = rawUser || {};
  const profile = user.profile || {};

  return {
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
}

async function syncUserDirectoryEntry(uid, rawUser) {
  if (!uid) {
    return;
  }

  const directoryData = buildUserDirectoryData(rawUser);
  const signature = JSON.stringify(directoryData);

  if (userDirectoryCache[uid] === signature) {
    return;
  }

  userDirectoryCache[uid] = signature;

  await db.ref(`userDirectory/${uid}`).set({
    ...directoryData,
    updatedAt: Date.now(),
  });
}

async function removeUserDirectoryEntry(uid) {
  if (!uid) {
    return;
  }

  delete userDirectoryCache[uid];
  await db.ref(`userDirectory/${uid}`).remove();
}

function startDeviceMapListener() {
  const indexRef = db.ref("system/devices_by_ieee");

  const upsertDevice = (snap) => {
    const deviceId = String(snap.key || "").trim();
    const value = snap.val() || {};
    const uid = String(value.uid || "").trim();
    const homeId = String(value.homeId || "").trim();

    if (!deviceId) {
      return;
    }

    if (!uid || !homeId) {
      delete deviceMap[deviceId];
      return;
    }

    deviceMap[deviceId] = { uid, homeId };
  };

  const removeDevice = (snap) => {
    const deviceId = String(snap.key || "").trim();

    if (deviceId) {
      delete deviceMap[deviceId];
    }
  };

  indexRef.on("child_added", upsertDevice);
  indexRef.on("child_changed", upsertDevice);
  indexRef.on("child_removed", removeDevice);
}

async function startBackendDataCache() {
  if (backendDataCacheStarted) {
    return;
  }

  backendDataCacheStarted = true;
  startDeviceMapListener();

  const accountsRef = db.ref("accounts");
  const sharedRef = db.ref("sharedByHome");

  const upsertAccount = (snap) => {
    const uid = String(snap.key || "").trim();

    if (!uid) {
      return;
    }

    const account = snap.val() || {};
    const previousAccount = accountCache.get(uid) || null;

    accountCache.set(uid, account);

    if (previousAccount) {
      void handleAlarmRelevantAccountChange(
        uid,
        previousAccount,
        account,
      );
    }

    void syncUserDirectoryEntry(uid, account).catch((error) => {
      console.log(
        "USER DIRECTORY SYNC ERROR:",
        uid,
        error.message,
      );
    });
  };

  const removeAccount = (snap) => {
    const uid = String(snap.key || "").trim();

    if (!uid) {
      return;
    }

    accountCache.delete(uid);

    for (const key of Array.from(
      localActiveAlarmIncidentMap.keys(),
    )) {
      if (key.startsWith(`${uid}|`)) {
        localActiveAlarmIncidentMap.delete(key);
      }
    }

    void removeUserDirectoryEntry(uid).catch((error) => {
      console.log(
        "USER DIRECTORY REMOVE ERROR:",
        uid,
        error.message,
      );
    });
  };

  const upsertSharedHome = (snap) => {
    const homeId = String(snap.key || "").trim();

    if (homeId) {
      sharedByHomeCache.set(homeId, snap.val() || {});
    }
  };

  const removeSharedHome = (snap) => {
    const homeId = String(snap.key || "").trim();

    if (homeId) {
      sharedByHomeCache.delete(homeId);
    }
  };

  accountsRef.on("child_added", upsertAccount);
  accountsRef.on("child_changed", upsertAccount);
  accountsRef.on("child_removed", removeAccount);

  sharedRef.on("child_added", upsertSharedHome);
  sharedRef.on("child_changed", upsertSharedHome);
  sharedRef.on("child_removed", removeSharedHome);

  // Bootstrap đúng một lần để các tác vụ khởi động có dữ liệu đầy đủ.
  const [accountsSnap, sharedSnap, deviceIndexSnap] =
    await Promise.all([
      accountsRef.once("value"),
      sharedRef.once("value"),
      db.ref("system/devices_by_ieee").once("value"),
    ]);

  const accounts = accountsSnap.val() || {};
  const sharedByHome = sharedSnap.val() || {};
  const deviceIndex = deviceIndexSnap.val() || {};

  const directorySyncTasks = [];

  for (const [uid, account] of Object.entries(accounts)) {
    const safeAccount = account || {};
    accountCache.set(uid, safeAccount);
    directorySyncTasks.push(
      syncUserDirectoryEntry(uid, safeAccount),
    );

    // Giữ tương thích với thiết bị cũ chưa có bản ghi trong
    // system/devices_by_ieee. Các thiết bị mới vẫn được cập nhật
    // realtime từ device index listener ở trên.
    const homes = safeAccount.homes || {};

    for (const [homeId, rawHome] of Object.entries(homes)) {
      const devices = rawHome?.devices || {};

      for (const deviceId of Object.keys(devices)) {
        if (!deviceMap[deviceId]) {
          deviceMap[deviceId] = { uid, homeId };
        }
      }
    }
  }

  for (const [homeId, members] of Object.entries(sharedByHome)) {
    sharedByHomeCache.set(homeId, members || {});
  }

  for (const [deviceId, rawEntry] of Object.entries(deviceIndex)) {
    const entry = rawEntry || {};
    const uid = String(entry.uid || "").trim();
    const homeId = String(entry.homeId || "").trim();

    if (uid && homeId) {
      deviceMap[deviceId] = { uid, homeId };
    }
  }

  await Promise.all(directorySyncTasks);

  console.log(
    "🗂️ BACKEND DATA CACHE READY:",
    `accounts=${accountCache.size}`,
    `homes=${sharedByHomeCache.size}`,
    `devices=${Object.keys(deviceMap).length}`,
  );
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
  return getDateKeyFromTimestamp(Date.now());
}

function getDateKeyFromTimestamp(timestamp) {
  const date = new Date(Number(timestamp || 0));
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");

  return `${yyyy}-${mm}-${dd}`;
}

function isValidHHMM(value) {
  return /^([01][0-9]|2[0-3]):[0-5][0-9]$/.test(
    String(value || "").trim(),
  );
}

function normalizeTimestamp(value) {
  const timestamp = Number(value);

  return Number.isFinite(timestamp) && timestamp > 0
    ? timestamp
    : 0;
}

function isTimeInPauseRange(startTime, endTime) {
  if (!startTime || !endTime) return false;
  return isNowInRange(startTime, endTime);
}

async function clearHomeAlarmPause(ownerUid, homeId, sharedUsers = null) {
  const updates = {
    [`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`]:
      null,
  };

  let resolvedSharedUsers = sharedUsers;

  if (!resolvedSharedUsers) {
    try {
      const sharedSnap = await db
        .ref(`sharedByHome/${homeId}`)
        .once("value");

      resolvedSharedUsers = sharedSnap.val() || {};
    } catch (_) {
      resolvedSharedUsers = {};
    }
  }

  for (const sharedUid of Object.keys(resolvedSharedUsers || {})) {
    if (sharedUid === ownerUid) {
      continue;
    }

    updates[
      `accounts/${sharedUid}/sharedHomes/${homeId}/alarmPauseToday`
    ] = null;
  }

  await db.ref().update(updates);
}

async function isHomeAlarmPausedToday(ownerUid, homeId) {
  try {
    const snap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
      .once("value");

    const pause = snap.val();

    if (!pause) return false;

    const now = Date.now();
    const startAt = normalizeTimestamp(pause.startAt);
    const endAt = normalizeTimestamp(pause.endAt);

    if (startAt > 0 && endAt > startAt) {
      if (now >= startAt && now < endAt) {
        return true;
      }

      if (now >= endAt) {
        try {
          await clearHomeAlarmPause(ownerUid, homeId);

          console.log(
            "🧹 ALARM PAUSE REMOVED:",
            ownerUid,
            homeId,
          );
        } catch (_) { }
      }

      return false;
    }

    // Dữ liệu cũ chưa có startAt/endAt: giữ tương thích tạm thời.
    const today = getTodayKey();

    if (pause.date !== today) {
      try {
        await clearHomeAlarmPause(ownerUid, homeId);
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

    const current = toMin(getCurrentHHMM());
    const start = toMin(pause.start);
    const end = toMin(pause.end);

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
        await clearHomeAlarmPause(ownerUid, homeId);

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

async function canReceiveAlarm(
  uid,
  homeId,
  ownerUid = uid,
  options = {},
) {
  try {
    const settingSnap = await db
      .ref(`accounts/${uid}/alarmSettings/${homeId}/enabled`)
      .once("value");

    const enabled = settingSnap.val();

    // Mặc định là bật, chỉ tắt khi user chủ động set false.
    if (enabled === false) {
      return false;
    }

    const respectPause = options?.respectPause !== false;

    // Tạm tắt Alarm hôm nay chỉ áp dụng cho Alarm theo giờ.
    // Mode Bảo vệ và cảnh báo khẩn cấp không bị chặn bởi pause.
    if (respectPause) {
      const paused = await isHomeAlarmPausedToday(ownerUid, homeId);

      if (paused) {
        console.log("⏸️ HOME ALARM PAUSED:", ownerUid, homeId);
        return false;
      }
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

    queueOrderedListCleanup(
      `home_notifications:${uid}`,
      listRef,
      HOME_NOTIFICATION_STORAGE_LIMIT,
    );

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

function getLocalActiveAlarmIncidentKey(
  receiverUid,
  targetKey,
) {
  return `${receiverUid}|${targetKey}`;
}

function setLocalActiveAlarmIncident(
  receiverUid,
  incidentId,
  incident,
) {
  const targetKey = String(
    incident?.targetKey || "",
  ).trim();

  if (!receiverUid || !incidentId || !targetKey) {
    return;
  }

  localActiveAlarmIncidentMap.set(
    getLocalActiveAlarmIncidentKey(
      receiverUid,
      targetKey,
    ),
    {
      incidentId,
      incident,
    },
  );
}

function removeLocalActiveAlarmIncident(
  receiverUid,
  targetKey,
) {
  const cleanTargetKey = String(
    targetKey || "",
  ).trim();

  if (!receiverUid || !cleanTargetKey) {
    return;
  }

  localActiveAlarmIncidentMap.delete(
    getLocalActiveAlarmIncidentKey(
      receiverUid,
      cleanTargetKey,
    ),
  );
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

  const retryPrefix = `${uid}_${incidentId}_`;

  for (const retryKey of Array.from(
    alarmIncidentStageRetryCountMap.keys(),
  )) {
    if (retryKey.startsWith(retryPrefix)) {
      alarmIncidentStageRetryCountMap.delete(retryKey);
    }
  }
}

function getAlarmStagePriority(stage) {
  const priorities = {
    detected: 0,
    notification: 0,
    alarm: 1,
    fullscreen_siren: 1,
    siren: 2,
    calling: 3,
  };

  return priorities[String(stage || "")] ?? -1;
}

function queueAlarmIncidentAdvance(
  receiverUid,
  incidentId,
  targetStage,
) {
  const lockKey = `${receiverUid}_${incidentId}`;
  const current = alarmIncidentQueuedStageMap.get(lockKey);

  if (
    !current ||
    getAlarmStagePriority(targetStage) >
      getAlarmStagePriority(current)
  ) {
    alarmIncidentQueuedStageMap.set(lockKey, targetStage);
  }
}

function getAlarmStageRetryKey(
  receiverUid,
  incidentId,
  targetStage,
) {
  return `${receiverUid}_${incidentId}_${targetStage}`;
}

function resetAlarmStageRetry(
  receiverUid,
  incidentId,
  targetStage,
) {
  alarmIncidentStageRetryCountMap.delete(
    getAlarmStageRetryKey(
      receiverUid,
      incidentId,
      targetStage,
    ),
  );
}

function scheduleAlarmIncidentStageRetry(
  receiverUid,
  incidentId,
  targetStage,
) {
  const retryKey = getAlarmStageRetryKey(
    receiverUid,
    incidentId,
    targetStage,
  );
  const retryCount =
    Number(alarmIncidentStageRetryCountMap.get(retryKey) || 0) + 1;

  if (retryCount > ALARM_STAGE_MAX_RETRY_COUNT) {
    console.log(
      "⚠️ ALARM STAGE RETRY LIMIT:",
      receiverUid,
      incidentId,
      targetStage,
    );
    return;
  }

  alarmIncidentStageRetryCountMap.set(
    retryKey,
    retryCount,
  );

  const timerKey = getAlarmIncidentTimerKey(
    receiverUid,
    incidentId,
  );
  const timers = alarmIncidentTimerMap[timerKey] || {};
  const slot = `retry_${targetStage}`;

  if (timers[slot]) {
    clearTimeout(timers[slot]);
  }

  timers[slot] = setTimeout(() => {
    const latestTimers =
      alarmIncidentTimerMap[timerKey] || {};
    delete latestTimers[slot];
    alarmIncidentTimerMap[timerKey] = latestTimers;

    void advanceAlarmIncidentToStage(
      receiverUid,
      incidentId,
      targetStage,
    );
  }, ALARM_STAGE_RETRY_DELAY_MS);

  alarmIncidentTimerMap[timerKey] = timers;

  console.log(
    "🔁 ALARM STAGE RETRY SCHEDULED:",
    receiverUid,
    incidentId,
    targetStage,
    `attempt=${retryCount}`,
  );
}

async function retryInitialAlarmIncidentPush(
  receiverUid,
  incidentId,
  stage,
  flowType,
) {
  const incidentRef = db.ref(
    `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
  );
  const snap = await incidentRef.once("value");
  const incident = snap.val();

  if (!incident || incident.status !== "active") {
    resetAlarmStageRetry(
      receiverUid,
      incidentId,
      stage,
    );
    return;
  }

  let items = normalizeAlarmIncidentItems(incident.items);

  if (flowType === "security") {
    const validation =
      await validateAndResolveSecurityIncident(
        receiverUid,
        incidentId,
        incident,
        { reasonHint: `retry_${stage}` },
      );

    if (!validation.active) {
      return;
    }

    items = validation.items;
  }

  const sent = await sendAlarmStageSummary(
    receiverUid,
    items,
    {
      incidentId,
      stage,
      flowType,
    },
  );

  if (!sent) {
    scheduleInitialAlarmIncidentPushRetry(
      receiverUid,
      incidentId,
      stage,
      flowType,
    );
    return;
  }

  resetAlarmStageRetry(
    receiverUid,
    incidentId,
    stage,
  );

  await incidentRef.update({
    initialPushSentAt: Date.now(),
    updatedAt: Date.now(),
  });
}

function scheduleInitialAlarmIncidentPushRetry(
  receiverUid,
  incidentId,
  stage,
  flowType,
) {
  const retryKey = getAlarmStageRetryKey(
    receiverUid,
    incidentId,
    stage,
  );
  const retryCount =
    Number(alarmIncidentStageRetryCountMap.get(retryKey) || 0) + 1;

  if (retryCount > ALARM_STAGE_MAX_RETRY_COUNT) {
    console.log(
      "⚠️ INITIAL ALARM PUSH RETRY LIMIT:",
      receiverUid,
      incidentId,
      stage,
    );
    return;
  }

  alarmIncidentStageRetryCountMap.set(
    retryKey,
    retryCount,
  );

  const timerKey = getAlarmIncidentTimerKey(
    receiverUid,
    incidentId,
  );
  const timers = alarmIncidentTimerMap[timerKey] || {};
  const slot = `retry_initial_${stage}`;

  if (timers[slot]) {
    clearTimeout(timers[slot]);
  }

  timers[slot] = setTimeout(() => {
    const latestTimers =
      alarmIncidentTimerMap[timerKey] || {};
    delete latestTimers[slot];
    alarmIncidentTimerMap[timerKey] = latestTimers;

    void retryInitialAlarmIncidentPush(
      receiverUid,
      incidentId,
      stage,
      flowType,
    );
  }, ALARM_STAGE_RETRY_DELAY_MS);

  alarmIncidentTimerMap[timerKey] = timers;
}

async function withAlarmIncidentStartLock(
  lockKey,
  callback,
) {
  const previous =
    alarmIncidentStartPromiseMap.get(lockKey) ||
    Promise.resolve();

  let releaseCurrent;
  const currentGate = new Promise((resolve) => {
    releaseCurrent = resolve;
  });
  const currentTail = previous
    .catch(() => {})
    .then(() => currentGate);

  alarmIncidentStartPromiseMap.set(
    lockKey,
    currentTail,
  );

  await previous.catch(() => {});

  try {
    return await callback();
  } finally {
    releaseCurrent();

    if (
      alarmIncidentStartPromiseMap.get(lockKey) ===
      currentTail
    ) {
      alarmIncidentStartPromiseMap.delete(lockKey);
    }
  }
}

function getAlarmIncidentItemIdentity(item) {
  return [
    String(item?.ownerUid || "").trim(),
    String(item?.homeId || "").trim(),
    String(item?.deviceId || item?.deviceName || "").trim(),
    String(item?.type || "").trim(),
    String(item?.alarmSource || "scheduled_alarm").trim(),
    String(item?.reason || "").trim(),
  ].join("|");
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
        deviceId: String(item.deviceId || "").trim(),
        deviceName: String(item.deviceName || "").trim(),
        type: String(item.type || "").trim(),
        reason,
        repeatMinutes: normalizeRepeatMinutes(
          item.repeatMinutes,
        ),
        nextAlarm: String(item.nextAlarm || "").trim(),
        alarmSource:
          String(item.alarmSource || "scheduled_alarm").trim() ||
          "scheduled_alarm",
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

// ================= INCIDENT VALIDATION =================
// Incident an ninh được hủy theo sự kiện thay vì polling dày.
// Tất cả kiểm tra thường dùng cache; Firebase chỉ được đọc bù khi
// event đến quá sớm và cache chưa kịp nhận incident vừa tạo.
const alarmHomeValidationTimerMap = new Map();

function getCachedAccountData(uid) {
  return accountCache.get(String(uid || "").trim()) || null;
}

function getCachedHomeData(ownerUid, homeId) {
  const ownerAccount = getCachedAccountData(ownerUid);

  if (!ownerAccount) {
    return null;
  }

  return ownerAccount?.homes?.[homeId] || null;
}

function isAlarmPauseActiveFromData(pause) {
  if (!pause || typeof pause !== "object") {
    return false;
  }

  const now = Date.now();
  const startAt = normalizeTimestamp(pause.startAt);
  const endAt = normalizeTimestamp(pause.endAt);

  if (startAt > 0 && endAt > startAt) {
    return now >= startAt && now < endAt;
  }

  if (
    String(pause.date || "") !== getTodayKey() ||
    !isValidHHMM(pause.start) ||
    !isValidHHMM(pause.end)
  ) {
    return false;
  }

  return isNowInRange(pause.start, pause.end);
}

function getIncidentDeviceContext(home, item) {
  const devices = home?.devices || {};
  const requestedDeviceId = String(
    item?.deviceId || "",
  ).trim();

  if (
    requestedDeviceId &&
    devices[requestedDeviceId]
  ) {
    return {
      deviceId: requestedDeviceId,
      device: devices[requestedDeviceId],
    };
  }

  const requestedName = String(
    item?.deviceName || "",
  ).trim();
  const requestedType = String(
    item?.type || "",
  ).trim();

  for (const [deviceId, rawDevice] of Object.entries(devices)) {
    const device = rawDevice || {};
    const deviceName = String(
      device.name || deviceId,
    ).trim();
    const deviceType = String(
      device.type || "",
    ).trim();

    if (
      requestedName &&
      deviceName === requestedName &&
      (!requestedType || deviceType === requestedType)
    ) {
      return { deviceId, device };
    }
  }

  return null;
}

function isSecurityIncidentItemStillUnsafe(
  home,
  item,
) {
  const context = getIncidentDeviceContext(home, item);

  // Không tự hủy khi không định danh được thiết bị của incident cũ.
  // Watchdog sẽ thử lại sau; đây là fail-safe để không bỏ sót Alarm thật.
  if (!context) {
    return true;
  }

  const device = context.device || {};
  const deviceType = String(
    item?.type || device.type || "",
  ).trim();
  const reason = String(item?.reason || "")
    .trim()
    .toLowerCase();

  const isTamperIncident =
    reason.includes("bị tháo") ||
    reason.includes("tamper") ||
    reason.includes("cạy");

  if (isTamperIncident) {
    return device.tamper === true;
  }

  if (
    deviceType === "door" ||
    deviceType === "window" ||
    deviceType === "gate"
  ) {
    return device.contact === false;
  }

  if (
    deviceType === "door_lock" ||
    deviceType === "lock"
  ) {
    return normalizeLockState(device) === "unlocked";
  }

  if (
    deviceType === "motion" ||
    deviceType === "presence"
  ) {
    return (
      isActiveSignal(device.occupancy) ||
      isActiveSignal(device.motion) ||
      isActiveSignal(device.presence)
    );
  }

  // Rung và kính vỡ là event tức thời, không có trạng thái clear đáng tin cậy.
  // Chúng chỉ bị hủy khi Mode/lịch không còn hiệu lực hoặc người dùng xử lý.
  if (
    deviceType === "vibration" ||
    deviceType === "glass_break"
  ) {
    return true;
  }

  return Boolean(
    getUnsafeSecurityReason(
      String(item?.deviceName || context.deviceId),
      deviceType,
      device,
    ),
  );
}

async function isSecurityIncidentSourceActive({
  receiverUid,
  ownerUid,
  homeId,
  item,
  home,
  receiverAccount,
}) {
  const source = String(
    item?.alarmSource || "scheduled_alarm",
  ).trim();

  if (
    receiverAccount?.alarmSettings?.[homeId]?.enabled ===
    false
  ) {
    return {
      active: false,
      reason: "alarm_disabled",
    };
  }

  if (source === "security_mode") {
    return {
      active:
        normalizeHomeSecurityMode(home?.securityMode) ===
        "armed",
      reason: "security_mode_normal",
    };
  }

  if (isAlarmPauseActiveFromData(home?.alarmPauseToday)) {
    return {
      active: false,
      reason: "alarm_paused",
    };
  }

  const context = getIncidentDeviceContext(home, item);

  // Incident cũ không có deviceId: nếu đang ở Mode Bảo vệ thì ưu tiên
  // giữ incident thay vì kết luận nhầm rằng lịch đã hết.
  if (!context) {
    if (
      normalizeHomeSecurityMode(home?.securityMode) ===
      "armed"
    ) {
      return { active: true, reason: "" };
    }

    return {
      active: true,
      reason: "device_unavailable",
    };
  }

  const alarm = await resolveDeviceAlarmForReceiver(
    receiverUid,
    homeId,
    context.deviceId,
    home,
    receiverAccount || {},
  );

  if (!alarm || alarm.enabled !== true) {
    return {
      active: false,
      reason: "alarm_schedule_disabled",
    };
  }

  if (!isAlarmAllowedToday(alarm)) {
    return {
      active: false,
      reason: "alarm_day_inactive",
    };
  }

  if (!isNowInRange(alarm.start, alarm.end)) {
    return {
      active: false,
      reason: "alarm_time_inactive",
    };
  }

  return { active: true, reason: "" };
}

async function evaluateSecurityIncident(
  receiverUid,
  incident,
  { homeOverride = null } = {},
) {
  const normalizedItems = normalizeAlarmIncidentItems(
    incident?.items,
  );

  if (incident?.flowType === "emergency") {
    return {
      active: true,
      items: normalizedItems,
      reason: "",
    };
  }

  const ownerUid = String(
    incident?.ownerUid || receiverUid,
  ).trim();
  const homeId = String(incident?.homeId || "").trim();
  const ownerAccount = getCachedAccountData(ownerUid);
  const receiverAccount =
    getCachedAccountData(receiverUid);

  if (!homeId) {
    return {
      active: false,
      items: [],
      reason: "home_missing",
    };
  }

  if (
    receiverUid !== ownerUid &&
    receiverAccount &&
    !receiverAccount?.sharedHomes?.[homeId]
  ) {
    return {
      active: false,
      items: [],
      reason: "home_access_removed",
    };
  }

  let home = homeOverride;

  if (!home) {
    home = ownerAccount?.homes?.[homeId] || null;
  }

  if (!home) {
    // Tài khoản đã có trong cache nhưng nhà không còn tồn tại.
    if (ownerAccount) {
      return {
        active: false,
        items: [],
        reason: "home_removed",
      };
    }

    // Cache chưa sẵn sàng: giữ Alarm để tránh hủy nhầm.
    return {
      active: true,
      items: normalizedItems,
      reason: "home_unavailable",
    };
  }

  if (normalizedItems.length === 0) {
    return {
      active: false,
      items: [],
      reason: "incident_items_empty",
    };
  }

  const validItems = [];
  let firstInactiveReason = "condition_cleared";

  for (const item of normalizedItems) {
    const sourceResult =
      await isSecurityIncidentSourceActive({
        receiverUid,
        ownerUid,
        homeId,
        item,
        home,
        receiverAccount: receiverAccount || {},
      });

    if (!sourceResult.active) {
      firstInactiveReason =
        sourceResult.reason || firstInactiveReason;
      continue;
    }

    if (!isSecurityIncidentItemStillUnsafe(home, item)) {
      firstInactiveReason = "device_state_resolved";
      continue;
    }

    const context = getIncidentDeviceContext(home, item);

    validItems.push({
      ...item,
      deviceId:
        String(item.deviceId || "").trim() ||
        String(context?.deviceId || "").trim(),
    });
  }

  return {
    active: validItems.length > 0,
    items: validItems,
    reason:
      validItems.length > 0
        ? ""
        : firstInactiveReason,
  };
}

function haveAlarmIncidentItemsChanged(oldItems, newItems) {
  return JSON.stringify(
    normalizeAlarmIncidentItems(oldItems),
  ) !== JSON.stringify(
    normalizeAlarmIncidentItems(newItems),
  );
}

async function validateAndResolveSecurityIncident(
  receiverUid,
  incidentId,
  incident,
  {
    homeOverride = null,
    reasonHint = "condition_changed",
  } = {},
) {
  const lockKey = `${receiverUid}|${incidentId}`;
  const existingPromise =
    alarmIncidentValidationPromiseMap.get(lockKey);

  if (existingPromise) {
    return existingPromise;
  }

  if (
    !incident ||
    incident.status !== "active" ||
    incident.flowType === "emergency"
  ) {
    return {
      active: incident?.status === "active",
      items: normalizeAlarmIncidentItems(
        incident?.items,
      ),
    };
  }

  const validationPromise = (async () => {
    try {
      const result = await evaluateSecurityIncident(
        receiverUid,
        incident,
        { homeOverride },
      );

      if (result.active) {
        if (
          haveAlarmIncidentItemsChanged(
            incident.items,
            result.items,
          )
        ) {
          const updatedAt = Date.now();

          await db
            .ref(
              `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
            )
            .update({
              items: result.items,
              reasons: result.items.map(
                (item) => item.reason,
              ),
              updatedAt,
            });

          setLocalActiveAlarmIncident(
            receiverUid,
            incidentId,
            {
              ...incident,
              items: result.items,
              reasons: result.items.map(
                (item) => item.reason,
              ),
              updatedAt,
            },
          );
        } else {
          setLocalActiveAlarmIncident(
            receiverUid,
            incidentId,
            incident,
          );
        }

        return result;
      }

      const ownerUid = String(
        incident.ownerUid || receiverUid,
      ).trim();
      const homeId = String(incident.homeId || "").trim();
      const action = String(
        result.reason || reasonHint || "condition_cleared",
      ).trim();

      await resolveAlarmIncidentForReceiver({
        receiverUid,
        incidentId,
        ownerUid,
        homeId,
        resolvedBy: "safehome_backend",
        action,
      });

      console.log(
        "🧹 ALARM INCIDENT AUTO RESOLVED:",
        receiverUid,
        incidentId,
        ownerUid,
        homeId,
        action,
      );

      return {
        active: false,
        items: [],
        reason: action,
      };
    } catch (error) {
      console.log(
        "ALARM INCIDENT VALIDATION ERROR:",
        receiverUid,
        incidentId,
        error.message,
      );

      // Khi không kiểm tra được, giữ incident thay vì bỏ Alarm thật.
      return {
        active: true,
        items: normalizeAlarmIncidentItems(
          incident?.items,
        ),
      };
    }
  })();

  alarmIncidentValidationPromiseMap.set(
    lockKey,
    validationPromise,
  );

  try {
    return await validationPromise;
  } finally {
    if (
      alarmIncidentValidationPromiseMap.get(lockKey) ===
      validationPromise
    ) {
      alarmIncidentValidationPromiseMap.delete(lockKey);
    }
  }
}

async function loadActiveSecurityIncidentForReceiver(
  receiverUid,
  ownerUid,
  homeId,
) {
  const targetKey = getAlarmIncidentTargetKey(
    receiverUid,
    ownerUid,
    homeId,
    "security",
  );
  const localKey = getLocalActiveAlarmIncidentKey(
    receiverUid,
    targetKey,
  );
  const localActive =
    localActiveAlarmIncidentMap.get(localKey);

  if (
    localActive?.incident?.status === "active" &&
    localActive?.incident?.flowType !== "emergency"
  ) {
    return localActive;
  }

  const account = getCachedAccountData(receiverUid);
  let incidentId = String(
    account?.activeAlarmIncidentByTarget?.[targetKey] ||
    "",
  ).trim();
  let incident = incidentId
    ? account?.alarmIncidents?.[incidentId]
    : null;

  if (
    incident?.status === "active" &&
    incident?.flowType !== "emergency"
  ) {
    const result = { incidentId, incident };
    localActiveAlarmIncidentMap.set(localKey, result);
    return result;
  }

  // Khi tài khoản đã có trong cache và không có index active,
  // không đọc Firebase thêm. Incident vừa tạo đã được ghi vào local map.
  if (account) {
    return null;
  }

  // Chỉ đọc bù trong giai đoạn cache chưa sẵn sàng.
  const incidentIdSnap = await db
    .ref(
      `accounts/${receiverUid}/activeAlarmIncidentByTarget/${targetKey}`,
    )
    .once("value");

  incidentId = String(incidentIdSnap.val() || "").trim();

  if (!incidentId) {
    return null;
  }

  const incidentSnap = await db
    .ref(
      `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
    )
    .once("value");

  incident = incidentSnap.val();

  if (
    !incident ||
    incident.status !== "active" ||
    incident.flowType === "emergency"
  ) {
    return null;
  }

  const result = { incidentId, incident };
  localActiveAlarmIncidentMap.set(localKey, result);
  return result;
}

async function validateSecurityIncidentsForHome(
  ownerUid,
  homeId,
  reasonHint,
  {
    receiverUid = "",
    homeOverride = null,
  } = {},
) {
  const receiverUids = new Set();
  const cleanReceiverUid = String(receiverUid || "").trim();

  if (cleanReceiverUid) {
    receiverUids.add(cleanReceiverUid);
  } else {
    receiverUids.add(ownerUid);

    const sharedMembers =
      sharedByHomeCache.get(homeId) || {};

    for (const sharedUid of Object.keys(sharedMembers)) {
      const cleanUid = String(sharedUid || "").trim();

      if (cleanUid) {
        receiverUids.add(cleanUid);
      }
    }
  }

  for (const targetReceiverUid of receiverUids) {
    try {
      const active =
        await loadActiveSecurityIncidentForReceiver(
          targetReceiverUid,
          ownerUid,
          homeId,
        );

      if (!active) {
        continue;
      }

      await validateAndResolveSecurityIncident(
        targetReceiverUid,
        active.incidentId,
        active.incident,
        {
          homeOverride,
          reasonHint,
        },
      );
    } catch (error) {
      console.log(
        "HOME INCIDENT VALIDATION ERROR:",
        targetReceiverUid,
        ownerUid,
        homeId,
        error.message,
      );
    }
  }
}

function queueSecurityIncidentValidationForHome(
  ownerUid,
  homeId,
  reasonHint,
  {
    receiverUid = "",
    delayMs = 250,
  } = {},
) {
  const key = [
    ownerUid,
    homeId,
    receiverUid || "all",
  ].join("|");
  const oldTimer = alarmHomeValidationTimerMap.get(key);

  if (oldTimer) {
    clearTimeout(oldTimer);
  }

  const timer = setTimeout(() => {
    alarmHomeValidationTimerMap.delete(key);

    void validateSecurityIncidentsForHome(
      ownerUid,
      homeId,
      reasonHint,
      { receiverUid },
    );
  }, Math.max(0, delayMs));

  alarmHomeValidationTimerMap.set(key, timer);
}

function getOwnedHomeAlarmControlSignature(home) {
  const deviceAlarms = {};

  for (const [deviceId, device] of Object.entries(
    home?.devices || {},
  )) {
    if (device?.alarm) {
      deviceAlarms[deviceId] = device.alarm;
    }
  }

  return JSON.stringify({
    securityMode: normalizeHomeSecurityMode(
      home?.securityMode,
    ),
    alarmPauseToday: home?.alarmPauseToday || null,
    alarm: home?.alarm || null,
    scheduleAlarms: home?.schedules?.alarms || null,
    deviceAlarms,
  });
}

function getReceiverHomeAlarmControlSignature(
  account,
  homeId,
) {
  const customHome = account?.customRules?.[homeId] || {};
  const customDeviceAlarms = {};

  for (const [deviceId, deviceRule] of Object.entries(
    customHome?.devices || {},
  )) {
    if (deviceRule?.alarm) {
      customDeviceAlarms[deviceId] = deviceRule.alarm;
    }
  }

  return JSON.stringify({
    enabled:
      account?.alarmSettings?.[homeId]?.enabled ?? null,
    mode: customHome?.mode || "home",
    customDeviceAlarms,
    sharedAlarmEnabled:
      account?.sharedHomes?.[homeId]?.alarmEnabled ?? null,
  });
}

async function handleAlarmRelevantAccountChange(
  uid,
  previousAccount,
  nextAccount,
) {
  try {
    const previousHomes = previousAccount?.homes || {};
    const nextHomes = nextAccount?.homes || {};
    const ownedHomeIds = new Set([
      ...Object.keys(previousHomes),
      ...Object.keys(nextHomes),
    ]);

    for (const homeId of ownedHomeIds) {
      const previousSignature =
        getOwnedHomeAlarmControlSignature(
          previousHomes[homeId],
        );
      const nextSignature =
        getOwnedHomeAlarmControlSignature(
          nextHomes[homeId],
        );

      if (previousSignature !== nextSignature) {
        queueSecurityIncidentValidationForHome(
          uid,
          homeId,
          "home_alarm_control_changed",
        );
      }
    }

    const receiverHomeIds = new Set([
      ...Object.keys(previousAccount?.alarmSettings || {}),
      ...Object.keys(nextAccount?.alarmSettings || {}),
      ...Object.keys(previousAccount?.customRules || {}),
      ...Object.keys(nextAccount?.customRules || {}),
      ...Object.keys(previousAccount?.sharedHomes || {}),
      ...Object.keys(nextAccount?.sharedHomes || {}),
    ]);

    for (const homeId of receiverHomeIds) {
      const previousSignature =
        getReceiverHomeAlarmControlSignature(
          previousAccount,
          homeId,
        );
      const nextSignature =
        getReceiverHomeAlarmControlSignature(
          nextAccount,
          homeId,
        );

      if (previousSignature === nextSignature) {
        continue;
      }

      const ownerUid = String(
        nextAccount?.sharedHomes?.[homeId]?.ownerUid ||
        previousAccount?.sharedHomes?.[homeId]?.ownerUid ||
        (nextAccount?.homes?.[homeId] ||
        previousAccount?.homes?.[homeId]
          ? uid
          : ""),
      ).trim();

      if (!ownerUid) {
        continue;
      }

      queueSecurityIncidentValidationForHome(
        ownerUid,
        homeId,
        "receiver_alarm_control_changed",
        { receiverUid: uid },
      );
    }
  } catch (error) {
    console.log(
      "ACCOUNT ALARM CONTROL CHANGE ERROR:",
      uid,
      error.message,
    );
  }
}

async function runAlarmIncidentWatchdog() {
  try {
    let checked = 0;
    const accounts = getCachedAccountsObject();

    for (const [receiverUid, account] of Object.entries(accounts)) {
      const incidents = account?.alarmIncidents || {};

      for (const [incidentId, incident] of Object.entries(incidents)) {
        if (
          incident?.status !== "active" ||
          incident?.flowType === "emergency"
        ) {
          continue;
        }

        checked++;

        await validateAndResolveSecurityIncident(
          receiverUid,
          incidentId,
          incident,
          { reasonHint: "watchdog_validation" },
        );
      }
    }

    if (checked > 0) {
      console.log(
        "🧭 ALARM INCIDENT WATCHDOG:",
        `checked=${checked}`,
      );
    }
  } catch (error) {
    console.log(
      "ALARM INCIDENT WATCHDOG ERROR:",
      error.message,
    );
  }
}

function startAlarmIncidentWatchdog() {
  if (alarmIncidentWatchdogTimer) {
    return;
  }

  alarmIncidentWatchdogTimer = setInterval(
    () => {
      void runAlarmIncidentWatchdog();
    },
    ALARM_INCIDENT_WATCHDOG_INTERVAL_MS,
  );

  console.log(
    "🧭 ALARM INCIDENT WATCHDOG STARTED:",
    `interval=${ALARM_INCIDENT_WATCHDOG_INTERVAL_MS / 1000}s`,
  );
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
        {
          respectPause:
            String(item.alarmSource || "scheduled_alarm") ===
            "scheduled_alarm",
        },
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
        receiverUid: String(uid || ""),
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
        apns: {
          headers: {
            "apns-priority": "5",
            "apns-push-type": "background",
          },
          payload: {
            aps: {
              "content-available": 1,
            },
          },
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

  setLocalActiveAlarmIncident(
    receiverUid,
    incidentId,
    incident,
  );

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
    queueAlarmIncidentAdvance(
      receiverUid,
      incidentId,
      targetStage,
    );
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
      let items = normalizeAlarmIncidentItems(
        incident.items,
      );

      if (flowType === "security") {
        const validation =
          await validateAndResolveSecurityIncident(
            receiverUid,
            incidentId,
            incident,
            {
              reasonHint: `before_${nextStage}`,
            },
          );

        if (!validation.active) {
          return;
        }

        items = validation.items;
      }

      const now = Date.now();

      if (nextStage === "alarm") {
        const sent = await sendAlarmStageSummary(
          receiverUid,
          items,
          {
            incidentId,
            stage: "alarm",
            flowType,
          },
        );

        if (!sent) {
          scheduleAlarmIncidentStageRetry(
            receiverUid,
            incidentId,
            "alarm",
          );
          return;
        }

        resetAlarmStageRetry(
          receiverUid,
          incidentId,
          "alarm",
        );

        await incidentRef.update({
          stage: "alarm",
          alarmSentAt: now,
          updatedAt: now,
        });
      } else if (nextStage === "siren") {
        const sent = await sendAlarmStageSummary(
          receiverUid,
          items,
          {
            incidentId,
            stage: "siren",
            flowType,
          },
        );

        if (!sent) {
          scheduleAlarmIncidentStageRetry(
            receiverUid,
            incidentId,
            "siren",
          );
          return;
        }

        resetAlarmStageRetry(
          receiverUid,
          incidentId,
          "siren",
        );

        await incidentRef.update({
          stage: "siren",
          sirenSentAt: now,
          updatedAt: now,
        });
      } else if (
        nextStage === "fullscreen_siren"
      ) {
        const sent = await sendAlarmStageSummary(
          receiverUid,
          items,
          {
            incidentId,
            stage: "fullscreen_siren",
            flowType,
          },
        );

        if (!sent) {
          scheduleAlarmIncidentStageRetry(
            receiverUid,
            incidentId,
            "fullscreen_siren",
          );
          return;
        }

        resetAlarmStageRetry(
          receiverUid,
          incidentId,
          "fullscreen_siren",
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

    const queuedStage =
      alarmIncidentQueuedStageMap.get(lockKey);

    if (queuedStage) {
      alarmIncidentQueuedStageMap.delete(lockKey);

      setImmediate(() => {
        void advanceAlarmIncidentToStage(
          receiverUid,
          incidentId,
          queuedStage,
        );
      });
    }
  }
}

function isPersistentEmergencyIncidentItem(item) {
  const type = String(item?.type || "").trim();

  return (
    type === "smoke" ||
    type === "heat" ||
    type === "carbon_monoxide" ||
    type === "gas" ||
    type === "water_leak" ||
    type === "flood"
  );
}

function isEmergencyIncidentItemStillUnsafe(
  home,
  item,
) {
  const context = getIncidentDeviceContext(home, item);

  if (!context) {
    return null;
  }

  const device = context.device || {};
  const type = String(
    item?.type || device.type || "",
  ).trim();
  const reason = String(item?.reason || "")
    .trim()
    .toLowerCase();

  if (
    reason.includes("bị tháo") ||
    reason.includes("tamper") ||
    reason.includes("cạy")
  ) {
    return device.tamper === true;
  }

  if (type === "smoke") {
    return isActiveSignal(device.smoke);
  }

  if (type === "heat") {
    return (
      isActiveSignal(device.heat) ||
      isActiveSignal(device.heat_alarm) ||
      isActiveSignal(
        device.high_temperature_alarm,
      )
    );
  }

  if (type === "carbon_monoxide") {
    return (
      isActiveSignal(device.carbon_monoxide) ||
      isActiveSignal(device.co_alarm)
    );
  }

  if (type === "gas") {
    return (
      isActiveSignal(device.gas) ||
      isActiveSignal(device.gas_alarm)
    );
  }

  if (
    type === "water_leak" ||
    type === "flood"
  ) {
    return (
      isActiveSignal(device.water_leak) ||
      isActiveSignal(device.leak) ||
      isActiveSignal(device.water)
    );
  }

  return false;
}

async function evaluatePersistentEmergencyIncident(
  incident,
) {
  const items = normalizeAlarmIncidentItems(
    incident?.items,
  );
  const persistentItems = items.filter(
    isPersistentEmergencyIncidentItem,
  );

  if (persistentItems.length === 0) {
    return {
      hasPersistentItems: false,
      activeItems: [],
      unknownItems: [],
    };
  }

  const ownerUid = String(
    incident?.ownerUid || "",
  ).trim();
  const homeId = String(
    incident?.homeId || "",
  ).trim();

  let home = getCachedHomeData(ownerUid, homeId);

  if (!home && ownerUid && homeId) {
    try {
      const homeSnap = await db
        .ref(`accounts/${ownerUid}/homes/${homeId}`)
        .once("value");
      home = homeSnap.val();
    } catch (_) { }
  }

  if (!home) {
    return {
      hasPersistentItems: true,
      activeItems: [],
      unknownItems: persistentItems,
    };
  }

  const activeItems = [];
  const unknownItems = [];

  for (const item of persistentItems) {
    const unsafe = isEmergencyIncidentItemStillUnsafe(
      home,
      item,
    );

    if (unsafe === true) {
      activeItems.push(item);
    } else if (unsafe === null) {
      unknownItems.push(item);
    }
  }

  return {
    hasPersistentItems: true,
    activeItems,
    unknownItems,
  };
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

    // Incident an ninh chỉ kết thúc khi điều kiện thực tế đã hết,
    // Mode/lịch không còn hiệu lực hoặc người dùng xử lý. Không để
    // auto-expire tạo incident mới trong khi cùng một cửa vẫn đang mở.
    if (incident.flowType !== "emergency") {
      const validation =
        await validateAndResolveSecurityIncident(
          receiverUid,
          incidentId,
          incident,
          { reasonHint: "auto_expire_validation" },
        );

      if (!validation.active) {
        return;
      }

      const now = Date.now();
      const nextExpireAt =
        now + ALARM_INCIDENT_AUTO_EXPIRE_MS;

      await incidentRef.update({
        items: validation.items,
        reasons: validation.items.map(
          (item) => item.reason,
        ),
        expireAt: nextExpireAt,
        updatedAt: now,
      });

      const key = getAlarmIncidentTimerKey(
        receiverUid,
        incidentId,
      );
      const timers = alarmIncidentTimerMap[key] || {};

      if (timers.expire) {
        clearTimeout(timers.expire);
      }

      timers.expire = setTimeout(
        () => {
          void expireAlarmIncident(
            receiverUid,
            incidentId,
          );
        },
        ALARM_INCIDENT_AUTO_EXPIRE_MS,
      );

      alarmIncidentTimerMap[key] = timers;

      setLocalActiveAlarmIncident(
        receiverUid,
        incidentId,
        {
          ...incident,
          items: validation.items,
          reasons: validation.items.map(
            (item) => item.reason,
          ),
          expireAt: nextExpireAt,
          updatedAt: now,
        },
      );

      console.log(
        "⏳ SECURITY INCIDENT KEPT ACTIVE:",
        receiverUid,
        incidentId,
      );
      return;
    }

    const emergencyValidation =
      await evaluatePersistentEmergencyIncident(
        incident,
      );

    if (
      emergencyValidation.hasPersistentItems &&
      (
        emergencyValidation.activeItems.length > 0 ||
        emergencyValidation.unknownItems.length > 0
      )
    ) {
      const keptItems = normalizeAlarmIncidentItems([
        ...emergencyValidation.activeItems,
        ...emergencyValidation.unknownItems,
      ]);
      const now = Date.now();
      const nextExpireAt =
        now + ALARM_INCIDENT_AUTO_EXPIRE_MS;

      await incidentRef.update({
        items: keptItems,
        reasons: keptItems.map(
          (item) => item.reason,
        ),
        expireAt: nextExpireAt,
        updatedAt: now,
      });

      const key = getAlarmIncidentTimerKey(
        receiverUid,
        incidentId,
      );
      const timers = alarmIncidentTimerMap[key] || {};

      if (timers.expire) {
        clearTimeout(timers.expire);
      }

      timers.expire = setTimeout(
        () => {
          void expireAlarmIncident(
            receiverUid,
            incidentId,
          );
        },
        ALARM_INCIDENT_AUTO_EXPIRE_MS,
      );

      alarmIncidentTimerMap[key] = timers;

      console.log(
        "⏳ EMERGENCY INCIDENT KEPT ACTIVE:",
        receiverUid,
        incidentId,
        `active=${emergencyValidation.activeItems.length}`,
        `unknown=${emergencyValidation.unknownItems.length}`,
      );
      return;
    }

    if (
      emergencyValidation.hasPersistentItems &&
      emergencyValidation.activeItems.length === 0 &&
      emergencyValidation.unknownItems.length === 0
    ) {
      await resolveAlarmIncidentForReceiver({
        receiverUid,
        incidentId,
        ownerUid: String(
          incident.ownerUid || receiverUid,
        ),
        homeId: String(incident.homeId || ""),
        resolvedBy: "safehome_backend",
        action: "emergency_condition_cleared",
      });
      return;
    }

    // SOS và các event tức thời không có trạng thái duy trì sẽ hết hạn
    // theo thời gian như cũ.
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
    removeLocalActiveAlarmIncident(
      receiverUid,
      targetKey,
    );
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

  for (const originalGroupedItems of groups.values()) {
    const firstItem = originalGroupedItems[0];
    const ownerUid = firstItem.ownerUid || uid;
    const homeId = firstItem.homeId;
    const homeName = firstItem.homeName || homeId;
    const flowType =
      getAlarmIncidentFlowType(originalGroupedItems);
    const targetKey = getAlarmIncidentTargetKey(
      uid,
      ownerUid,
      homeId,
      flowType,
    );
    const startLockKey = `${uid}|${targetKey}`;

    await withAlarmIncidentStartLock(
      startLockKey,
      async () => {
        let groupedItems = normalizeAlarmIncidentItems(
          originalGroupedItems,
        );

        if (flowType === "security") {
          const preCreateValidation =
            await evaluateSecurityIncident(
              uid,
              {
                receiverUid: uid,
                ownerUid,
                homeId,
                flowType,
                status: "active",
                items: groupedItems,
              },
            );

          if (!preCreateValidation.active) {
            console.log(
              "🧹 ALARM INCIDENT SKIPPED, CONDITION CLEARED:",
              uid,
              ownerUid,
              homeId,
              preCreateValidation.reason,
            );
            return;
          }

          groupedItems = preCreateValidation.items;
        }

        const respectPause = groupedItems.some((item) => {
          return (
            String(
              item.alarmSource || "scheduled_alarm",
            ) === "scheduled_alarm"
          );
        });

        const enabled = flowType === "emergency"
          ? true
          : await canReceiveAlarm(
              uid,
              homeId,
              ownerUid,
              { respectPause },
            );

        if (!enabled) {
          console.log(
            "🔕 ALARM INCIDENT NOT CREATED:",
            uid,
            homeId,
            flowType,
          );
          return;
        }

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

          if (flowType === "security") {
            const existingItems = normalizeAlarmIncidentItems(
              active.incident.items,
            );
            const existingKeys = new Set(
              existingItems.map(getAlarmIncidentItemIdentity),
            );
            const newItems = groupedItems.filter((item) => {
              return !existingKeys.has(
                getAlarmIncidentItemIdentity(item),
              );
            });
            const repeatBaseAt = Number(
              active.incident.lastRepeatedAt ||
              active.incident.detectedAt ||
              active.incident.createdAt ||
              0,
            );
            const repeatedItems = groupedItems.filter((item) => {
              const repeatMinutes =
                normalizeRepeatMinutes(item.repeatMinutes);

              return (
                existingKeys.has(
                  getAlarmIncidentItemIdentity(item),
                ) &&
                repeatMinutes > 0 &&
                (
                  repeatBaseAt <= 0 ||
                  now - repeatBaseAt >=
                    repeatMinutes * 60 * 1000
                )
              );
            });
            const mergedItems = normalizeAlarmIncidentItems([
              ...existingItems,
              ...groupedItems,
            ]);
            const updateData = {
              items: mergedItems,
              reasons: mergedItems.map(
                (item) => item.reason,
              ),
              updatedAt: now,
            };

            if (newItems.length > 0) {
              updateData.lastNewConditionAt = now;
            }

            if (repeatedItems.length > 0) {
              updateData.lastRepeatedAt = now;
              updateData.repeatCount =
                Number(active.incident.repeatCount || 0) + 1;
            }

            await db
              .ref(
                `accounts/${uid}/alarmIncidents/${active.incidentId}`,
              )
              .update(updateData);

            const updatedIncident = {
              ...active.incident,
              ...updateData,
            };

            setLocalActiveAlarmIncident(
              uid,
              active.incidentId,
              updatedIncident,
            );

            if (newItems.length > 0) {
              const sent = await sendAlarmStageSummary(
                uid,
                newItems,
                {
                  incidentId: active.incidentId,
                  stage: "alarm",
                  flowType,
                },
              );

              if (!sent) {
                scheduleInitialAlarmIncidentPushRetry(
                  uid,
                  active.incidentId,
                  "alarm",
                  flowType,
                );
              }
            } else if (repeatedItems.length > 0) {
              const sent = await sendAlarmStageSummary(
                uid,
                repeatedItems,
                {
                  incidentId: active.incidentId,
                  stage: "alarm",
                  flowType,
                },
              );

              if (!sent) {
                scheduleInitialAlarmIncidentPushRetry(
                  uid,
                  active.incidentId,
                  "alarm",
                  flowType,
                );
              }
            }

            console.log(
              "➕ SECURITY INCIDENT UPDATED:",
              uid,
              active.incidentId,
              `new=${newItems.length}`,
              `repeat=${repeatedItems.length}`,
              `items=${mergedItems.length}`,
            );

            return;
          }

          const mayMerge =
            activeAgeMs >= 0 &&
            activeAgeMs <= EMERGENCY_MERGE_WINDOW_MS;

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
                reasons: mergedItems.map(
                  (item) => item.reason,
                ),
                updatedAt: now,
              });

            setLocalActiveAlarmIncident(
              uid,
              active.incidentId,
              {
                ...active.incident,
                items: mergedItems,
                reasons: mergedItems.map(
                  (item) => item.reason,
                ),
                updatedAt: now,
              },
            );

            console.log(
              "➕ ALARM INCIDENT MERGED:",
              uid,
              active.incidentId,
              flowType,
              mergedItems.length,
            );

            return;
          }

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
              supersededReason: "new_emergency_trigger",
              callStatus:
                active.incident.callStatus === "not_started"
                  ? "not_started"
                  : "superseded",
              updatedAt: now,
            });

          removeLocalActiveAlarmIncident(
            uid,
            targetKey,
          );

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
          return;
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
          reasons: groupedItems.map(
            (item) => item.reason,
          ),
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

        setLocalActiveAlarmIncident(
          uid,
          incidentId,
          incident,
        );

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
            groupedItems
              .map((item) => item.reason)
              .join(", "),
          entityType: "home",
          entityId: homeId,
        });

        scheduleAlarmIncidentStages(
          uid,
          incidentId,
          incident,
        );

        const initialSent = await sendAlarmStageSummary(
          uid,
          groupedItems,
          {
            incidentId,
            stage: initialStage,
            flowType,
          },
        );

        if (initialSent) {
          resetAlarmStageRetry(
            uid,
            incidentId,
            initialStage,
          );
          await db
            .ref(
              `accounts/${uid}/alarmIncidents/${incidentId}`,
            )
            .update({
              initialPushSentAt: Date.now(),
              updatedAt: Date.now(),
            });
        } else {
          scheduleInitialAlarmIncidentPushRetry(
            uid,
            incidentId,
            initialStage,
            flowType,
          );
        }

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
      },
    );
  }
}

async function resumeActiveAlarmIncidents() {
  try {
    const accounts = getCachedAccountsObject();
    let resumed = 0;

    for (const [uid, account] of Object.entries(accounts)) {
      const incidents = account?.alarmIncidents || {};

      for (const [incidentId, incident] of Object.entries(incidents)) {
        if (incident?.status !== "active") {
          continue;
        }

        let resumableIncident = incident;

        if (incident?.flowType !== "emergency") {
          const validation =
            await validateAndResolveSecurityIncident(
              uid,
              incidentId,
              incident,
              { reasonHint: "backend_restart_validation" },
            );

          if (!validation.active) {
            continue;
          }

          resumableIncident = {
            ...incident,
            items: validation.items,
          };
        }

        setLocalActiveAlarmIncident(
          uid,
          incidentId,
          resumableIncident,
        );

        scheduleAlarmIncidentStages(
          uid,
          incidentId,
          resumableIncident,
        );

        const resumedInitialStage =
          resumableIncident.flowType === "emergency"
            ? "notification"
            : "detected";

        if (
          !resumableIncident.initialPushSentAt &&
          String(resumableIncident.stage || resumedInitialStage) ===
            resumedInitialStage
        ) {
          scheduleInitialAlarmIncidentPushRetry(
            uid,
            incidentId,
            resumedInitialStage,
            resumableIncident.flowType === "emergency"
              ? "emergency"
              : "security",
          );
        }

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

async function resolveAlarmIncidentForReceiver({
  receiverUid,
  incidentId,
  ownerUid,
  homeId,
  resolvedBy,
  action,
}) {
  const incidentRef = db.ref(
    `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
  );

  const incidentSnap = await incidentRef.once("value");
  const incident = incidentSnap.val();

  if (
    !incident ||
    incident.status !== "active" ||
    String(incident.ownerUid || "") !== ownerUid ||
    String(incident.homeId || "") !== homeId
  ) {
    return false;
  }

  const now = Date.now();
  const updates = {
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/status`]:
      "resolved",
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/stage`]:
      "resolved",
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/resolvedAt`]:
      now,
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/resolvedBy`]:
      resolvedBy,
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/resolutionAction`]:
      action,
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/updatedAt`]:
      now,
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/homeSirenStatus`]:
      "stop_requested",
  };

  const targetKey = String(
    incident.targetKey || "",
  ).trim();

  if (targetKey) {
    updates[
      `accounts/${receiverUid}/activeAlarmIncidentByTarget/${targetKey}`
    ] = null;
  }

  await db.ref().update(updates);

  removeLocalActiveAlarmIncident(
    receiverUid,
    targetKey,
  );

  clearAlarmIncidentTimers(
    receiverUid,
    incidentId,
  );

  await sendAlarmResolvedPush({
    uid: receiverUid,
    incidentId,
    homeId,
    resolvedBy,
    action,
  });

  console.log(
    "✅ ALARM INCIDENT RESOLVED FOR RECEIVER:",
    receiverUid,
    incidentId,
    ownerUid,
    homeId,
    action,
  );

  return true;
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
    const accounts = getCachedAccountsObject();
    const now = Date.now();

    for (const [uid, user] of Object.entries(accounts)) {
      const homes = user.homes || {};

      for (const [homeId, home] of Object.entries(homes)) {
        const pause = home.alarmPauseToday;

        if (!pause) continue;

        const startAt = normalizeTimestamp(pause.startAt);
        const endAt = normalizeTimestamp(pause.endAt);

        if (startAt > 0 && endAt > startAt) {
          if (now >= endAt) {
            const sharedSnap = await db
              .ref(`sharedByHome/${homeId}`)
              .once("value");

            const sharedUsers = sharedSnap.val() || {};

            await clearHomeAlarmPause(
              uid,
              homeId,
              sharedUsers,
            );

            console.log(
              "🧹 EXPIRED ALARM PAUSE REMOVED:",
              uid,
              homeId,
            );
          }

          continue;
        }

        // Dữ liệu cũ chưa có startAt/endAt: giữ cleanup cũ.
        const today = getTodayKey();

        if (pause.date !== today) {
          const sharedSnap = await db
            .ref(`sharedByHome/${homeId}`)
            .once("value");

          const sharedUsers = sharedSnap.val() || {};

          await clearHomeAlarmPause(
            uid,
            homeId,
            sharedUsers,
          );

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
          const current = toMin(getCurrentHHMM());

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
            const sharedSnap = await db
              .ref(`sharedByHome/${homeId}`)
              .once("value");

            const sharedUsers = sharedSnap.val() || {};

            await clearHomeAlarmPause(
              uid,
              homeId,
              sharedUsers,
            );

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
    const accounts = getCachedAccountsObject();
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
async function addHomeEvent(
  uid,
  homeId,
  deviceId,
  deviceName,
  text,
  type = "status",
) {
  try {
    const now = Date.now();
    const eventsRef = db.ref(
      `accounts/${uid}/homes/${homeId}/events`,
    );
    const eventRef = eventsRef.push();

    await eventRef.set({
      time: now,
      deviceId,
      deviceName,
      text,
      type,
    });

    queueOrderedListCleanup(
      `home_events:${uid}:${homeId}`,
      eventsRef,
      HOME_EVENT_STORAGE_LIMIT,
    );

    console.log(
      "🏠 HOME EVENT:",
      homeId,
      deviceName,
      text,
    );
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

    const notificationsRef =
      deviceRef.child("notifications");
    const notifRef = notificationsRef.push();

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

    queueOrderedListCleanup(
      `device_notifications:${uid}:${homeId}:${deviceId}`,
      notificationsRef,
      DEVICE_NOTIFICATION_STORAGE_LIMIT,
    );

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

function normalizeAlarmDays(value) {
  const days = [];

  if (Array.isArray(value)) {
    for (const rawDay of value) {
      const day = Number.parseInt(rawDay, 10);

      if (
        Number.isFinite(day) &&
        day >= 1 &&
        day <= 7 &&
        !days.includes(day)
      ) {
        days.push(day);
      }
    }
  }

  days.sort((a, b) => a - b);

  // Dữ liệu Alarm cũ chưa có days sẽ được hiểu là chạy hằng ngày.
  return days.length > 0
    ? days
    : [1, 2, 3, 4, 5, 6, 7];
}

function getCurrentAlarmWeekdayForSchedule(startTime, endTime) {
  const now = new Date();
  const jsDay = now.getDay(); // 0 = Sunday
  let weekday = jsDay === 0 ? 7 : jsDay;

  const nowMin =
    now.getHours() * 60 + now.getMinutes();
  const start = toMin(startTime || "23:00");
  const end = toMin(endTime || "06:00");

  // Lịch đi qua 00:00:
  // 01:00 thứ Ba vẫn thuộc ca Alarm bắt đầu từ tối thứ Hai.
  if (start > end && nowMin < end) {
    weekday -= 1;

    if (weekday < 1) {
      weekday = 7;
    }
  }

  return weekday;
}

function isAlarmAllowedToday(alarm) {
  const days = normalizeAlarmDays(alarm?.days);
  const activeWeekday = getCurrentAlarmWeekdayForSchedule(
    alarm?.start,
    alarm?.end,
  );

  return days.includes(activeWeekday);
}

function getWeekdayFromDate(date) {
  const jsDay = date.getDay();

  return jsDay === 0 ? 7 : jsDay;
}

function getDateStart(timestamp) {
  const date = new Date(Number(timestamp || Date.now()));

  return new Date(
    date.getFullYear(),
    date.getMonth(),
    date.getDate(),
  );
}

function alarmInstanceOverlapsPause(
  alarm,
  pauseStartAt,
  pauseEndAt,
) {
  if (
    !alarm ||
    alarm.enabled !== true ||
    !isValidHHMM(alarm.start) ||
    !isValidHHMM(alarm.end)
  ) {
    return false;
  }

  const days = normalizeAlarmDays(alarm.days);
  const pauseStart = Number(pauseStartAt);
  const pauseEnd = Number(pauseEndAt);

  if (
    !Number.isFinite(pauseStart) ||
    !Number.isFinite(pauseEnd) ||
    pauseEnd <= pauseStart
  ) {
    return false;
  }

  const startMinute = toMin(alarm.start);
  const endMinute = toMin(alarm.end);
  const baseDate = getDateStart(pauseStart).getTime();

  // Kiểm tra từ hôm trước tới 2 ngày sau để bao phủ lịch qua đêm.
  for (let offset = -1; offset <= 2; offset++) {
    const startDate = new Date(
      baseDate + offset * 24 * 60 * 60 * 1000,
    );

    if (!days.includes(getWeekdayFromDate(startDate))) {
      continue;
    }

    const alarmStart =
      startDate.getTime() + startMinute * 60 * 1000;

    let alarmEnd =
      startDate.getTime() + endMinute * 60 * 1000;

    if (alarmEnd <= alarmStart) {
      alarmEnd += 24 * 60 * 60 * 1000;
    }

    if (pauseStart < alarmEnd && alarmStart < pauseEnd) {
      return true;
    }
  }

  return false;
}

function doesPauseOverlapEnabledAlarm(
  home,
  pauseStartAt,
  pauseEndAt,
) {
  const devices = home?.devices || {};

  for (const device of Object.values(devices)) {
    const alarm = device?.alarm;

    if (
      alarmInstanceOverlapsPause(
        alarm,
        pauseStartAt,
        pauseEndAt,
      )
    ) {
      return true;
    }
  }

  return false;
}

function normalizeSecurityModeRepeatMinutes(value) {
  const minutes = Number.parseInt(value || 0, 10);

  return [0, 15, 30, 60].includes(minutes)
    ? minutes
    : 0;
}

function getSecurityModeAlarmKey(
  receiverUid,
  ownerUid,
  homeId,
  deviceId,
) {
  return [
    "security_mode",
    receiverUid,
    ownerUid,
    homeId,
    deviceId,
  ].join("_");
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
    normalizeAlarmDays(alarm?.days).join("-"),
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
        deviceId,
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
        deviceId,
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
        deviceId,
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
        deviceId,
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
        deviceId,
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
        deviceId,
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
        deviceId,
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

  const securityModeArmed =
    normalizeHomeSecurityMode(
      homeData.securityMode,
    ) === "armed";

  const scheduleArmed =
    !securityModeArmed &&
    deviceAlarm?.enabled === true &&
    isAlarmAllowedToday(deviceAlarm) &&
    isNowInRange(
      deviceAlarm.start,
      deviceAlarm.end,
    );

  // Mode Bảo vệ (thủ công hoặc Auto Away) bảo vệ toàn bộ
  // sensor an ninh. Khi nhà ở Mode Bình thường, lịch riêng
  // của từng sensor mới được sử dụng.
  if (!securityModeArmed && !scheduleArmed) {
    return;
  }

  // Với Alarm theo giờ, phải chặn ngay tại điểm nhận event.
  // Nếu đợi tới lúc tạo incident thì cửa mở vẫn có thể ghi lastScheduleAlarmMap
  // hoặc lọt qua một nhịp xử lý realtime trước khi pause được áp dụng.
  if (scheduleArmed) {
    const canReceiveScheduledAlarm = await canReceiveAlarm(
      receiverUid,
      homeId,
      ownerUid,
      { respectPause: true },
    );

    if (!canReceiveScheduledAlarm) {
      return;
    }
  }

  const repeatMinutes = securityModeArmed
    ? normalizeSecurityModeRepeatMinutes(
        homeData.securityModeRepeatMinutes,
      )
    : normalizeRepeatMinutes(
        deviceAlarm?.repeatMinutes,
      );

  const nextAlarm =
    getNextAlarmTimeText(repeatMinutes);

  function rememberAlarmTrigger() {
    const alarmKey = securityModeArmed
      ? getSecurityModeAlarmKey(
          receiverUid,
          ownerUid,
          homeId,
          deviceId,
        )
      : getScheduleAlarmKey(
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
    rememberAlarmTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Cửa mở bất thường`,
      repeatMinutes,
      nextAlarm,
      alarmSource: securityModeArmed
        ? "security_mode"
        : "scheduled_alarm",
    });

    return;
  }

  if (
    updateData.tamper === true &&
    oldDevice.tamper !== true
  ) {
    rememberAlarmTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Thiết bị bị tháo`,
      repeatMinutes,
      nextAlarm,
      alarmSource: securityModeArmed
        ? "security_mode"
        : "scheduled_alarm",
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
    rememberAlarmTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Phát hiện chuyển động`,
      repeatMinutes,
      nextAlarm,
      alarmSource: securityModeArmed
        ? "security_mode"
        : "scheduled_alarm",
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
    rememberAlarmTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      type: deviceType,
      reason:
        deviceType === "glass_break"
          ? `${deviceName}: Phát hiện kính vỡ`
          : `${deviceName}: Phát hiện rung/chấn động`,
      repeatMinutes,
      nextAlarm,
      alarmSource: securityModeArmed
        ? "security_mode"
        : "scheduled_alarm",
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
    rememberAlarmTrigger();

    queueEventAlarm(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      type: deviceType,
      reason: `${deviceName}: Khóa đã mở`,
      repeatMinutes,
      nextAlarm,
      alarmSource: securityModeArmed
        ? "security_mode"
        : "scheduled_alarm",
    });
  }
}

async function checkScheduledAlarms() {
  console.log(
    "🚨 CHECK PER-DEVICE ALARM / SECURITY MODE",
  );

  try {
    const accounts = getCachedAccountsObject();
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

        const securityModeArmed =
          normalizeHomeSecurityMode(
            home?.securityMode,
          ) === "armed";

        const securityModeRepeatMinutes =
          normalizeSecurityModeRepeatMinutes(
            home?.securityModeRepeatMinutes,
          );

        const devices = home?.devices || {};

        for (const [deviceId, device] of Object.entries(devices)) {
          const deviceType = String(
            device?.type || "door",
          ).trim();

          if (!isSecurityDeviceType(deviceType)) {
            continue;
          }

          const securityModeAlarmKey =
            getSecurityModeAlarmKey(
              receiverUid,
              ownerUid,
              homeId,
              deviceId,
            );

          let alarmKey = "";
          let repeatMinutes = 0;

          if (securityModeArmed) {
            repeatMinutes =
              securityModeRepeatMinutes;

            // 0 nghĩa là chỉ báo một lần theo event hoặc ngay
            // lúc Mode được bật. Không cần quét lặp định kỳ.
            if (repeatMinutes === 0) {
              continue;
            }

            alarmKey = securityModeAlarmKey;
          } else {
            delete lastScheduleAlarmMap[
              securityModeAlarmKey
            ];

            const deviceAlarm =
              await resolveDeviceAlarmForReceiver(
                receiverUid,
                homeId,
                deviceId,
                home,
                receiverAccount,
              );

            if (
              !deviceAlarm ||
              deviceAlarm.enabled !== true
            ) {
              continue;
            }

            alarmKey = getScheduleAlarmKey(
              receiverUid,
              ownerUid,
              homeId,
              deviceId,
              deviceAlarm,
            );

            if (!isAlarmAllowedToday(deviceAlarm)) {
              delete lastScheduleAlarmMap[alarmKey];
              continue;
            }

            if (
              !isNowInRange(
                deviceAlarm.start,
                deviceAlarm.end,
              )
            ) {
              delete lastScheduleAlarmMap[alarmKey];
              continue;
            }

            repeatMinutes =
              normalizeRepeatMinutes(
                deviceAlarm.repeatMinutes,
              );
          }

          const canReceive = await canReceiveAlarm(
            receiverUid,
            homeId,
            ownerUid,
            {
              respectPause: !securityModeArmed,
            },
          );

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
            deviceId,
            deviceName,
            type: deviceType,
            reason,
            repeatMinutes,
            nextAlarm:
              getNextAlarmTimeText(repeatMinutes),
            alarmSource: securityModeArmed
              ? "security_mode"
              : "scheduled_alarm",
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
      "PER-DEVICE ALARM / SECURITY MODE ERROR:",
      error.message,
    );
  }
}

async function cleanupLegacySecurityScheduleState() {
  try {
    const accounts = getCachedAccountsObject();
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
    const repeatMinutes =
      normalizeSecurityModeRepeatMinutes(
        home.securityModeRepeatMinutes,
      );
    const nextAlarm =
      getNextAlarmTimeText(repeatMinutes);
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
        deviceId,
        deviceName,
        type: deviceType,
        reason,
        repeatMinutes,
        nextAlarm,
        alarmSource: "security_mode",
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

      const triggeredAt = Date.now();

      for (const item of alarmItems) {
        const alarmKey =
          getSecurityModeAlarmKey(
            receiverUid,
            ownerUid,
            homeId,
            item.deviceId,
          );

        lastScheduleAlarmMap[alarmKey] =
          triggeredAt;
      }
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
      securityModeLastValueMap.set(key, nextMode);

      // Sau khi backend khởi động lại, nếu nhà đang ở Mode Bảo vệ
      // thì phải kiểm tra trạng thái sensor hiện tại. Hàm tạo incident
      // đã có khóa và cơ chế merge nên không tạo Alarm trùng.
      if (nextMode === "armed") {
        setTimeout(() => {
          void triggerAlarmForUnsafeStateOnArmed(
            ownerUid,
            homeId,
          );
        }, 1000);
      }

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
      return;
    }

    if (
      previousMode === "armed" &&
      nextMode === "normal"
    ) {
      const cachedHome =
        getCachedHomeData(ownerUid, homeId) || {};

      void validateSecurityIncidentsForHome(
        ownerUid,
        homeId,
        "security_mode_normal",
        {
          homeOverride: {
            ...cachedHome,
            securityMode: "normal",
          },
        },
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

  // Cache đã được bootstrap trước khi monitor bắt đầu, nên không cần
  // tải lại toàn bộ /accounts chỉ để chụp trạng thái nền.
  const accounts = asObject(getCachedAccountsObject());

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
// Chỉ tự bật Mode Bảo vệ khi toàn bộ thành viên đủ điều kiện
// ở ngoài liên tục 60 giây.
const AUTO_AWAY_ARM_DELAY_MS = 60 * 1000;

// Chỉ tự chuyển về Bình thường khi có người ở trong nhà
// liên tục 30 giây.
const AUTO_AWAY_INSIDE_CONFIRM_MS = 30 * 1000;

// Sau khi Auto Away tự chuyển về Bình thường vì có người về,
// khóa không cho tự bật lại trong 2 phút.
const AUTO_AWAY_REARM_BLOCK_MS = 2 * 60 * 1000;

const AUTO_AWAY_SCAN_INTERVAL_MS = 10 * 1000;

// Khi user chủ động chuyển từ Bảo vệ về Bình thường,
// Auto Away chỉ tạm hoãn, không bị khóa vĩnh viễn.
// Sau thời gian này, nếu mọi thành viên đủ điều kiện vẫn ở ngoài,
// backend sẽ bắt đầu lại chu kỳ tự bật Bảo vệ.
const AUTO_AWAY_MANUAL_NORMAL_SNOOZE_MS = 2 * 60 * 1000;

// Chỉ dùng để hiển thị sức khỏe giám sát, không đổi inside/outside
// thành unknown và không loại thành viên khỏi Auto Away.
// Native geofence có thể im lặng nhiều giờ khi người dùng không
// đi qua ranh giới, đây là hành vi bình thường.
const AUTO_AWAY_MONITORING_HEALTH_STALE_MS =
  24 * 60 * 60 * 1000;
const IOS_STALE_PRESENCE_MAX_AGE_MS =
  24 * 60 * 60 * 1000;
// Nếu app/foreground service không ghi heartbeat nữa
// (máy shutdown, hết pin, app bị kill hoàn toàn),
// sau 12 phút backend sẽ coi vị trí là không xác định.
const ACCOUNT_SESSION_STALE_MS =
  12 * 60 * 1000;

let autoAwayTimer = null;
let autoAwayScanRunning = false;

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value
    : {};
}

function hasSecurityDevices(home) {
  const devices = asObject(home?.devices);

  return Object.values(devices).some((rawDevice) => {
    const device = asObject(rawDevice);
    const deviceType = String(
      device.type || "unknown",
    ).trim();

    return isSecurityDeviceType(deviceType);
  });
}

function getAccountSessionStatus(account, now) {
  const sessions = Object.values(
    asObject(account?.sessions),
  ).map((rawSession) => asObject(rawSession));

  if (sessions.length === 0) {
    return {
      active: false,
      connected: false,
      reason: "legacy_session_missing",
      freshestSeenAt: 0,
      appState: "",
      signedInSessionCount: 0,
    };
  }

  let signedInCount = 0;
  let active = false;
  let connected = false;
  let freshestSeenAt = 0;
  let freshestAppState = "";
  let freshestPlatform = "";

  for (const session of sessions) {
    if (session.signedIn !== true) {
      continue;
    }

    signedInCount++;

    const lastSeenAt = Math.max(
      Number(session.lastSeenAt || 0),
      Number(session.lastLoginAt || 0),
    );

    if (lastSeenAt >= freshestSeenAt) {
      freshestSeenAt = lastSeenAt;
      freshestAppState = String(
        session.appState || "",
      ).trim();
      freshestPlatform = String(
        session.platform || "",
      ).trim();
    }

    const sessionIsActive =
      lastSeenAt > 0 &&
      now - lastSeenAt <= ACCOUNT_SESSION_STALE_MS;

    if (!sessionIsActive) {
      continue;
    }

    active = true;

    if (session.connected === true) {
      connected = true;
    }
  }

  if (active) {
    return {
      active: true,
      connected,
      reason: "",
      freshestSeenAt,
      appState: freshestAppState,
      platform: freshestPlatform,
      signedInSessionCount: signedInCount,
    };
  }

  return {
    active: false,
    connected: false,
    reason:
      signedInCount > 0
        ? "session_stale"
        : "signed_out",
    freshestSeenAt,
    appState: freshestAppState,
    platform: freshestPlatform,
    signedInSessionCount: signedInCount,
  };
}


function normalizePresenceMonitoringWarnings(presence) {
  const value = asObject(presence);
  const warnings = new Set();
  const rawWarnings = value.monitoringWarnings;

  if (Array.isArray(rawWarnings)) {
    for (const rawWarning of rawWarnings) {
      const warning = String(rawWarning || "").trim();

      if (warning) {
        warnings.add(warning);
      }
    }
  } else if (
    rawWarnings &&
    typeof rawWarnings === "object"
  ) {
    for (const [rawWarning, enabled] of Object.entries(
      rawWarnings,
    )) {
      const warning = String(rawWarning || "").trim();

      if (warning && enabled === true) {
        warnings.add(warning);
      }
    }
  } else {
    const warning = String(rawWarnings || "").trim();

    if (warning) {
      warnings.add(warning);
    }
  }

  // Tương thích dữ liệu từ phiên bản app cũ.
  // Các trạng thái này chỉ là khuyến nghị, không được loại thành viên
  // khỏi phép tính Auto Away.
  if (value.batteryUnrestricted === false) {
    warnings.add("battery_optimization_recommended");
  }

  if (value.backgroundRestricted === true) {
    warnings.add("background_activity_restricted");
  }

  if (value.autoStartConfirmed === false) {
    warnings.add("auto_start_recommended");
  }

  const legacyReason = String(
    value.monitoringBlockingReason || "",
  ).trim();

  if (
    legacyReason === "battery_optimization_required"
  ) {
    warnings.add("battery_optimization_recommended");
  } else if (
    legacyReason === "background_restricted"
  ) {
    warnings.add("background_activity_restricted");
  } else if (
    legacyReason === "auto_start_required"
  ) {
    warnings.add("auto_start_recommended");
  }

  return Array.from(warnings).sort();
}

function monitoringWarningsToFirebaseMap(warnings) {
  const result = {};

  for (const rawWarning of Array.isArray(warnings)
    ? warnings
    : []) {
    const warning = String(rawWarning || "").trim();

    if (warning) {
      result[warning] = true;
    }
  }

  return Object.keys(result).length > 0
    ? result
    : null;
}

function getPresenceMonitoringAvailability(presence) {
  const value = asObject(presence);

  // locationAlwaysGranted là nguồn chuẩn của app hiện tại.
  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "locationAlwaysGranted",
    )
  ) {
    return value.locationAlwaysGranted === true;
  }

  // Tương thích ngắn hạn với bản app đã ghi monitoringAvailable.
  if (
    Object.prototype.hasOwnProperty.call(
      value,
      "monitoringAvailable",
    )
  ) {
    return value.monitoringAvailable === true;
  }

  // Dữ liệu rất cũ chưa có hai trường trên.
  return value.monitoringEligible !== false;
}

function getMemberPresenceStatus(
  accounts,
  memberUid,
  ownerUid,
  homeId,
  sessionStatus,
  now,
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

  const identityMatches =
    storedOwnerUid === ownerUid &&
    storedHomeId === homeId;

  const rawState = String(
    presence.state || "unknown",
  ).trim();

  const event = String(
    presence.event || "",
  ).trim();

  const storedMonitoringBlockingReason = String(
    presence.monitoringBlockingReason || "",
  ).trim();

  const monitoringWarnings =
    normalizePresenceMonitoringWarnings(presence);

  const monitoringAvailable =
    getPresenceMonitoringAvailability(presence);

  const hasSignedOutMarker =
    event === "signed_out" ||
    storedMonitoringBlockingReason === "signed_out";

  const sessionActive =
  sessionStatus?.active === true;

const sessionPlatform = String(
  sessionStatus?.platform || "",
).trim();

const hasKnownState =
  rawState === "inside" ||
  rawState === "outside";

const presenceUpdatedAt = Number(
  presence.updatedAt || 0,
);

const lastConfirmedAt = Math.max(
  Number(presence.lastConfirmedAt || 0),
  Number(presence.lastEventOccurredAt || 0),
  presenceUpdatedAt,
);

const sessionFreshestSeenAt = Number(
  sessionStatus?.freshestSeenAt || 0,
);

// Lấy lần hoạt động gần nhất từ cả session và geofence.
// Geofence enter/exit hợp lệ cũng gia hạn trạng thái iOS.
const iosFreshestActivityAt = Math.max(
  sessionFreshestSeenAt,
  lastConfirmedAt,
);

const iosPresenceExpired =
  sessionPlatform === "ios" &&
  (
    iosFreshestActivityAt <= 0 ||
    now - iosFreshestActivityAt >
      IOS_STALE_PRESENCE_MAX_AGE_MS
  );

// iOS được phép giữ trạng thái khi app chỉ đang suspend,
// nhưng không được giữ vô thời hạn.
const staleIosPresenceAllowed =
  !sessionActive &&
  !hasSignedOutMarker &&
  !iosPresenceExpired &&
  sessionPlatform === "ios" &&
  Number(sessionStatus?.signedInSessionCount || 0) > 0;

const sessionAllowsPresence =
  sessionActive || staleIosPresenceAllowed;

// Session đang hoạt động luôn thắng marker signed_out cũ,
// nhưng marker signed_out thật vẫn phải chặn trạng thái cũ.
const explicitlySignedOut =
  hasSignedOutMarker && !sessionActive;

const reactivatedAfterSignedOut =
  hasSignedOutMarker && sessionActive;

  // Chỉ là trạng thái sức khỏe để hiển thị/cảnh báo.
  // Không được đổi inside/outside thành unknown chỉ vì không có
  // heartbeat định kỳ trong lúc app chạy nền hoặc bị kết thúc.
  const monitoringHealthStale =
    monitoringAvailable &&
    hasKnownState &&
    lastConfirmedAt > 0 &&
    now - lastConfirmedAt >
      AUTO_AWAY_MONITORING_HEALTH_STALE_MS;

  const monitoringHealth =
    !monitoringAvailable
      ? "unavailable"
      : !hasKnownState
        ? "waiting_location"
        : monitoringHealthStale
          ? "stale"
          : "active";

  const monitoringHealthReason =
    monitoringHealth === "unavailable"
      ? (
          storedMonitoringBlockingReason ||
          "permission_required"
        )
      : monitoringHealth === "waiting_location"
        ? "location_not_confirmed"
        : monitoringHealth === "stale"
          ? "no_recent_confirmation"
          : "";

  // Chỉ quyền vị trí nền là điều kiện bắt buộc.
  // Pin/chạy nền/tự khởi động chỉ là cảnh báo.
  const monitoringEligible = monitoringAvailable;

  const monitoringBlockingReason =
    storedMonitoringBlockingReason === "signed_out"
      ? "signed_out"
      : monitoringAvailable
        ? ""
        : "permission_required";

  // Background/terminated dựa vào native geofence. Nếu không có
  // event mới thì trạng thái trước đó vẫn là trạng thái xác nhận
  // gần nhất; không được tự biến thành unknown sau 2 phút.
  const state =
    identityMatches &&
    sessionAllowsPresence &&
    !reactivatedAfterSignedOut &&
    hasKnownState
      ? rawState
      : "unknown";

  const eligibleForArming =
    identityMatches &&
    sessionAllowsPresence &&
    !reactivatedAfterSignedOut &&
    monitoringEligible &&
    hasKnownState;

  const unknownWhileMonitored =
    identityMatches &&
    sessionAllowsPresence &&
    monitoringEligible &&
    (
      reactivatedAfterSignedOut ||
      !hasKnownState
    );

  const sessionReason = sessionActive
    ? ""
    : explicitlySignedOut
      ? "signed_out"
      : staleIosPresenceAllowed
        ? "ios_background_geofence"
        : String(sessionStatus?.reason || "").trim();

  return {
    identityMatches,
    eligibleForArming,
    unknownWhileMonitored,
    sessionActive,
    sessionAllowsPresence,
    staleIosPresenceAllowed,
    sessionReason,
    reactivatedAfterSignedOut,
    needsSessionCleanup:
      identityMatches && !sessionAllowsPresence,
    state,
    rawState,
    event,
    monitoringEligible,
    monitoringAvailable,
    monitoringWarnings,
    monitoringBlockingReason,
    monitoringHealth,
    monitoringHealthReason,
    monitoringHealthStale,
    lastConfirmedAt,
    storedMonitoringEligible:
      presence.monitoringEligible === true,
    storedMonitoringAvailable:
      presence.monitoringAvailable === true,
    storedMonitoringBlockingReason,
    updatedAt: presenceUpdatedAt,
  };
}

function runtimeSignature(runtime) {
  const value = asObject(runtime);

  return JSON.stringify({
    status: String(value.status || ""),
    totalMemberCount: Number(value.totalMemberCount || 0),
    memberCount: Number(value.memberCount || 0),
    eligibleMemberCount: Number(
      value.eligibleMemberCount || 0,
    ),
    excludedCount: Number(value.excludedCount || 0),
    insideCount: Number(value.insideCount || 0),
    outsideCount: Number(value.outsideCount || 0),
    unknownCount: Number(value.unknownCount || 0),
    knownLocationCount: Number(
      value.knownLocationCount || 0,
    ),
    armingInsideCount: Number(
      value.armingInsideCount || 0,
    ),
    armingOutsideCount: Number(
      value.armingOutsideCount || 0,
    ),
    armingUnknownCount: Number(
      value.armingUnknownCount || 0,
    ),
    allOutsideSince: Number(value.allOutsideSince || 0),
    insideCandidateSince: Number(
      value.insideCandidateSince || 0,
    ),
    rearmBlockedUntil: Number(
      value.rearmBlockedUntil || 0,
    ),
    cycleArmed: value.cycleArmed === true,
    manualNormalSnoozeUntil: Number(
      value.manualNormalSnoozeUntil || 0,
    ),
    insideOverrideUid: String(
      value.insideOverrideUid || "",
    ),
    insideOverrideAt: Number(
      value.insideOverrideAt || 0,
    ),
  });
}

function buildRuntime({
  status,
  totalMemberCount,
  memberCount,
  eligibleMemberCount,
  excludedCount,
  insideCount,
  outsideCount,
  unknownCount,
  knownLocationCount,
  armingInsideCount,
  armingOutsideCount,
  armingUnknownCount,
  allOutsideSince,
  insideCandidateSince = 0,
  rearmBlockedUntil = 0,
  cycleArmed,
  manualNormalSnoozeUntil = 0,
  insideOverrideUid = "",
  insideOverrideAt = 0,
  now,
}) {
  const safeInsideCount = Number(insideCount || 0);
  const safeOutsideCount = Number(outsideCount || 0);
  const safeUnknownCount = Number(unknownCount || 0);
  const safeEligibleMemberCount = Number(
    eligibleMemberCount ?? memberCount ?? 0,
  );
  const safeKnownLocationCount = Number(
    knownLocationCount ??
      safeInsideCount + safeOutsideCount,
  );

  return {
    status,
    totalMemberCount,
    // memberCount giữ vai trò mẫu số hiển thị cũ.
    // Luôn dùng tổng thành viên thật để tránh UI hiện 2/2
    // khi thực tế là 2/3 và 1/3 chưa rõ vị trí.
    memberCount,
    eligibleMemberCount: safeEligibleMemberCount,
    excludedCount,
    insideCount: safeInsideCount,
    outsideCount: safeOutsideCount,
    unknownCount: safeUnknownCount,
    knownLocationCount: safeKnownLocationCount,
    armingInsideCount: Number(
      armingInsideCount ?? safeInsideCount,
    ),
    armingOutsideCount: Number(
      armingOutsideCount ?? safeOutsideCount,
    ),
    armingUnknownCount: Number(
      armingUnknownCount ?? Math.max(
        0,
        safeEligibleMemberCount -
          Number(armingInsideCount ?? safeInsideCount) -
          Number(armingOutsideCount ?? safeOutsideCount),
      ),
    ),
    allOutsideSince: allOutsideSince || null,
    insideCandidateSince:
      Number(insideCandidateSince || 0) || null,
    rearmBlockedUntil:
      Number(rearmBlockedUntil || 0) || null,
    cycleArmed: cycleArmed === true,
    manualNormalSnoozeUntil:
      Number(manualNormalSnoozeUntil || 0) || null,
    insideOverrideUid:
      String(insideOverrideUid || "").trim() || null,
    insideOverrideAt:
      Number(insideOverrideAt || 0) || null,
    updatedAt: now,
  };
}

function presenceSummarySignature(summary) {
  const value = asObject(summary);

  return JSON.stringify({
    totalMemberCount: Number(value.totalMemberCount || 0),
    signedInCount: Number(value.signedInCount || 0),
    onlineCount: Number(value.onlineCount || 0),
    connectedCount: Number(value.connectedCount || 0),
    memberCount: Number(value.memberCount || 0),
    eligibleMemberCount: Number(
      value.eligibleMemberCount || 0,
    ),
    excludedCount: Number(value.excludedCount || 0),
    insideCount: Number(value.insideCount || 0),
    outsideCount: Number(value.outsideCount || 0),
    unknownCount: Number(value.unknownCount || 0),
    knownLocationCount: Number(
      value.knownLocationCount || 0,
    ),
    armingInsideCount: Number(
      value.armingInsideCount || 0,
    ),
    armingOutsideCount: Number(
      value.armingOutsideCount || 0,
    ),
    armingUnknownCount: Number(
      value.armingUnknownCount || 0,
    ),
    unavailableCount: Number(value.unavailableCount || 0),
  });
}

function buildPresenceSummary({
  totalMemberCount,
  signedInCount,
  onlineCount,
  connectedCount,
  memberCount,
  eligibleMemberCount,
  excludedCount,
  insideCount,
  outsideCount,
  unknownCount,
  knownLocationCount,
  armingInsideCount,
  armingOutsideCount,
  armingUnknownCount,
  unavailableCount,
  now,
}) {
  return {
    totalMemberCount,
    signedInCount,
    onlineCount,
    connectedCount,
    // memberCount giữ tương thích cho UI cũ, nhưng phải là
    // tổng thành viên thật, không phải số người eligible.
    memberCount,
    eligibleMemberCount,
    excludedCount,
    insideCount,
    outsideCount,
    unknownCount,
    knownLocationCount,
    armingInsideCount,
    armingOutsideCount,
    armingUnknownCount,
    unavailableCount,
    updatedAt: now,
  };
}

function memberPresenceStatusSignature(statusMap) {
  const normalized = {};

  for (const memberUid of Object.keys(
    asObject(statusMap),
  ).sort()) {
    const value = asObject(statusMap[memberUid]);

    normalized[memberUid] = {
      online: value.online === true,
      connected: value.connected === true,
      state: String(value.state || "unknown"),
      locationKnown: value.locationKnown === true,
      monitoringEligible:
        value.monitoringEligible === true,
      monitoringAvailable:
        value.monitoringAvailable === true,
      monitoringWarnings: Array.isArray(
        value.monitoringWarnings,
      )
        ? [...value.monitoringWarnings].sort()
        : [],
      monitoringWarningReason: String(
        value.monitoringWarningReason || "",
      ),
      monitoringHealth: String(
        value.monitoringHealth || "",
      ),
      monitoringHealthReason: String(
        value.monitoringHealthReason || "",
      ),
      lastConfirmedAt: Number(
        value.lastConfirmedAt || 0,
      ),
      appState: String(value.appState || ""),
      reason: String(value.reason || ""),
      lastSeenAt: Number(value.lastSeenAt || 0),
    };
  }

  return JSON.stringify(normalized);
}

async function checkAutoAwayHomes(db) {
  if (autoAwayScanRunning) {
    return;
  }

  autoAwayScanRunning = true;

  try {
    const accounts = getCachedAccountsObject();
    const sharedByHome = getCachedSharedByHomeObject();
    const now = Date.now();
    const updates = {};
    const logs = [];
    const sessionStatusByUid = new Map();

    for (const [accountUid, rawAccount] of Object.entries(accounts)) {
      sessionStatusByUid.set(
        accountUid,
        getAccountSessionStatus(
          asObject(rawAccount),
          now,
        ),
      );
    }

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
        const memberPresenceStatusPath =
          `${homePath}/memberPresenceStatus`;

        const members = new Set([ownerUid]);
        const sharedMembers = asObject(sharedByHome[homeId]);

        for (const rawMemberUid of Object.keys(sharedMembers)) {
          const memberUid = String(rawMemberUid || "").trim();

          if (memberUid) {
            members.add(memberUid);
          }
        }

        const eligibleStates = [];
        const memberPresenceByUid = new Map();
        const nextMemberPresenceStatus = {};
        let excludedCount = 0;
        let monitoredUnknownCount = 0;
        let signedInCount = 0;
        let connectedCount = 0;

        for (const memberUid of members) {
          const sessionStatus =
            sessionStatusByUid.get(memberUid) ||
            getAccountSessionStatus(
              asObject(accounts?.[memberUid]),
              now,
            );

          const presenceStatus = getMemberPresenceStatus(
            accounts,
            memberUid,
            ownerUid,
            homeId,
            sessionStatus,
            now,
          );

          memberPresenceByUid.set(
            memberUid,
            presenceStatus,
          );

          // Online/offline lấy từ installation session hiện tại.
          // homePresence signed_out chỉ là marker vị trí của phiên
          // trước và không được giữ tài khoản offline sau khi login lại.
          const signedInForHome =
            sessionStatus.active === true;

          const presenceAvailableForHome =
            presenceStatus.sessionAllowsPresence === true;

          const connectedForHome =
            signedInForHome &&
            sessionStatus.connected === true;

          if (signedInForHome) {
            signedInCount++;
          }

          if (connectedForHome) {
            connectedCount++;
          }

          // Tự chuyển dữ liệu từ logic cũ: tối ưu pin, giới hạn nền
          // và tự khởi động chỉ còn là cảnh báo, không còn khóa Auto Away.
          const legacyWarningReason =
            presenceStatus.storedMonitoringBlockingReason ===
              "battery_optimization_required" ||
            presenceStatus.storedMonitoringBlockingReason ===
              "background_restricted" ||
            presenceStatus.storedMonitoringBlockingReason ===
              "auto_start_required";

          const shouldNormalizeLegacyMonitoring =
            presenceAvailableForHome &&
            presenceStatus.identityMatches === true &&
            presenceStatus.reactivatedAfterSignedOut !== true &&
            presenceStatus.monitoringAvailable === true &&
            (
              presenceStatus.storedMonitoringEligible !== true ||
              presenceStatus.storedMonitoringAvailable !== true ||
              legacyWarningReason
            );

          if (shouldNormalizeLegacyMonitoring) {
            const presencePath =
              `accounts/${memberUid}/homePresence/${homeId}`;

            updates[`${presencePath}/monitoringEligible`] = true;
            updates[`${presencePath}/monitoringAvailable`] = true;
            updates[`${presencePath}/monitoringWarnings`] =
              monitoringWarningsToFirebaseMap(
                presenceStatus.monitoringWarnings,
              );
            updates[`${presencePath}/monitoringWarningReason`] =
              presenceStatus.monitoringWarnings[0] || null;
            updates[`${presencePath}/monitoringBlockingReason`] =
              null;
            updates[`${presencePath}/monitoringCheckedAt`] = now;

            logs.push(
              `⚙️ AUTO AWAY MONITORING NORMALIZED: ${memberUid} ${homeId}`,
            );
          }

          const locationKnown =
            presenceAvailableForHome &&
            (
              presenceStatus.state === "inside" ||
              presenceStatus.state === "outside"
            );

          nextMemberPresenceStatus[memberUid] = {
            online: signedInForHome,
            connected: connectedForHome,
            state: locationKnown
              ? presenceStatus.state
              : "unknown",
            locationKnown,
            monitoringEligible:
              presenceAvailableForHome &&
              presenceStatus.monitoringAvailable === true,
            monitoringAvailable:
              presenceAvailableForHome &&
              presenceStatus.monitoringAvailable === true,
            monitoringWarnings:
              presenceAvailableForHome
                ? presenceStatus.monitoringWarnings
                : [],
            monitoringWarningReason:
              presenceAvailableForHome
                ? presenceStatus.monitoringWarnings[0] || ""
                : "",
            monitoringHealth:
              presenceAvailableForHome
                ? presenceStatus.monitoringHealth
                : "unavailable",
            monitoringHealthReason:
              presenceAvailableForHome
                ? presenceStatus.monitoringHealthReason
                : "signed_out",
            lastConfirmedAt:
              presenceAvailableForHome
                ? Number(
                    presenceStatus.lastConfirmedAt || 0,
                  )
                : 0,
            appState: signedInForHome
              ? String(
                  sessionStatus.appState || "",
                ).trim()
              : presenceStatus.staleIosPresenceAllowed
                ? "ios_background"
                : "signed_out",
            reason:
              presenceAvailableForHome
                ? presenceStatus.reactivatedAfterSignedOut
                  ? "session_reactivated"
                  : presenceStatus.staleIosPresenceAllowed
                    ? "ios_background_geofence"
                    : presenceStatus.monitoringAvailable === true
                      ? ""
                      : String(
                          presenceStatus.monitoringBlockingReason ||
                          "permission_required",
                        ).trim()
                : String(
                    presenceStatus.sessionReason || "signed_out",
                  ).trim(),
            lastSeenAt: Number(
              sessionStatus.freshestSeenAt || 0,
            ),
            updatedAt: now,
          };

          if (presenceStatus.needsSessionCleanup) {
            const reason =
              presenceStatus.sessionReason ||
              "session_stale";

            const presencePath =
              `accounts/${memberUid}/homePresence/${homeId}`;

            const cleanupChanged =
              presenceStatus.rawState !== "unknown" ||
              presenceStatus.event !== reason ||
              presenceStatus.storedMonitoringEligible !== false ||
              presenceStatus.storedMonitoringAvailable !== false ||
              presenceStatus.monitoringBlockingReason !== reason;

            if (cleanupChanged) {
              updates[`${presencePath}/state`] = "unknown";
              updates[`${presencePath}/event`] = reason;
              updates[`${presencePath}/source`] =
                "native_geofence";
              updates[`${presencePath}/updatedAt`] = now;
              updates[`${presencePath}/monitoringEligible`] = false;
              updates[`${presencePath}/monitoringAvailable`] = false;
              updates[`${presencePath}/monitoringWarnings`] = null;
              updates[`${presencePath}/monitoringWarningReason`] = null;
              updates[`${presencePath}/monitoringBlockingReason`] =
                reason;
              updates[`${presencePath}/monitoringCheckedAt`] = now;

              logs.push(
                `👤 SESSION INACTIVE → UNKNOWN: ${memberUid} ${homeId} reason=${reason}`,
              );
            }
          }

          if (!presenceStatus.eligibleForArming) {
            excludedCount++;

            if (presenceStatus.unknownWhileMonitored) {
              monitoredUnknownCount++;
            }

            continue;
          }

          eligibleStates.push(presenceStatus.state);
        }

        const totalMemberCount = members.size;

        // Các biến arming* chỉ dùng cho quyết định Auto Away.
        // Unknown không được tính là inside, cũng không được tính
        // là outside khi xét tự bật Bảo vệ.
        const memberCount = eligibleStates.length;
        const insideCount = eligibleStates.filter(
          (state) => state === "inside",
        ).length;
        const outsideCount = eligibleStates.filter(
          (state) => state === "outside",
        ).length;
        const armingUnknownCount = Math.max(
          0,
          memberCount - insideCount - outsideCount,
        );

        // Các biến display* dùng cho UI và timeline summary.
        // Mẫu số luôn là tổng thành viên thật để không hiện 2/2
        // khi thực tế là 2/3 trong nhà và 1/3 chưa rõ vị trí.
        const displayMemberCount = totalMemberCount;
        const displayInsideCount = Object.values(
          nextMemberPresenceStatus,
        ).filter((status) => {
          return asObject(status).state === "inside";
        }).length;
        const displayOutsideCount = Object.values(
          nextMemberPresenceStatus,
        ).filter((status) => {
          return asObject(status).state === "outside";
        }).length;
        const displayKnownLocationCount =
          displayInsideCount + displayOutsideCount;
        const displayUnknownCount = Math.max(
          0,
          totalMemberCount - displayKnownLocationCount,
        );
        const unavailableCount = displayUnknownCount;

        const runtimeCounts = {
          totalMemberCount,
          memberCount: displayMemberCount,
          eligibleMemberCount: memberCount,
          excludedCount,
          insideCount: displayInsideCount,
          outsideCount: displayOutsideCount,
          unknownCount: displayUnknownCount,
          knownLocationCount: displayKnownLocationCount,
          armingInsideCount: insideCount,
          armingOutsideCount: outsideCount,
          armingUnknownCount,
        };

        const currentPresenceSummary =
          asObject(home.presenceSummary);

        const nextPresenceSummary =
          buildPresenceSummary({
            totalMemberCount,
            signedInCount,
            onlineCount: signedInCount,
            connectedCount,
            memberCount: displayMemberCount,
            eligibleMemberCount: memberCount,
            excludedCount,
            insideCount: displayInsideCount,
            outsideCount: displayOutsideCount,
            unknownCount: displayUnknownCount,
            knownLocationCount: displayKnownLocationCount,
            armingInsideCount: insideCount,
            armingOutsideCount: outsideCount,
            armingUnknownCount,
            unavailableCount,
            now,
          });

        if (
          presenceSummarySignature(currentPresenceSummary) !==
          presenceSummarySignature(nextPresenceSummary)
        ) {
          updates[presenceSummaryPath] =
            nextPresenceSummary;
        }

        const currentMemberPresenceStatus =
          asObject(home.memberPresenceStatus);

        if (
          memberPresenceStatusSignature(
            currentMemberPresenceStatus,
          ) !==
          memberPresenceStatusSignature(
            nextMemberPresenceStatus,
          )
        ) {
          updates[memberPresenceStatusPath] =
            nextMemberPresenceStatus;
        }

        const securityMode =
          normalizeHomeSecurityMode(home.securityMode);
        const securityModeSource = String(
          home.securityModeSource || "",
        ).trim();

        // Bật Bảo vệ thủ công là khóa ưu tiên cao nhất:
        // Auto Away không được tự hạ về Bình thường và không ghi đè
        // nguồn manual khi user đã chủ động bật Bảo vệ.
        if (
          securityModeSource === "manual" &&
          securityMode === "armed"
        ) {
          const nextRuntime = buildRuntime({
            status: "manual_override",
            ...runtimeCounts,
            allOutsideSince: 0,
            cycleArmed: false,
            manualNormalSnoozeUntil: 0,
            insideOverrideUid: "",
            insideOverrideAt: 0,
            now,
          });

          if (
            runtimeSignature(runtime) !==
            runtimeSignature(nextRuntime)
          ) {
            updates[runtimePath] = nextRuntime;
          }

          continue;
        }

        // Chuyển về Bình thường thủ công không được khóa Auto Away
        // vĩnh viễn. Đây chỉ là một khoảng tạm hoãn ngắn để user
        // không bị hệ tự bật lại ngay lập tức.
        if (
          securityModeSource === "manual" &&
          securityMode === "normal"
        ) {
          updates[`${homePath}/securityModeSource`] = null;

          if (autoAway.enabled === true) {
            const snoozeUntil =
              now + AUTO_AWAY_MANUAL_NORMAL_SNOOZE_MS;

            const nextRuntime = buildRuntime({
              status: "manual_normal_snooze",
              ...runtimeCounts,
              allOutsideSince: 0,
              cycleArmed: false,
              manualNormalSnoozeUntil: snoozeUntil,
              insideOverrideUid: "",
              insideOverrideAt: 0,
              now,
            });

            if (
              runtimeSignature(runtime) !==
              runtimeSignature(nextRuntime)
            ) {
              updates[runtimePath] = nextRuntime;
            }

            logs.push(
              `⏸️ AUTO AWAY MANUAL NORMAL SNOOZE: ${ownerUid} ${homeId} until=${snoozeUntil}`,
            );

            continue;
          }
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

        // Nhà không có sensor thuộc nhóm an ninh không được tự bật
        // Mode Bảo vệ. Sensor môi trường/hạ tầng không được tính.
        if (!hasSecurityDevices(home)) {
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
              `🏠 AUTO AWAY NO SECURITY DEVICES → NORMAL: ${ownerUid} ${homeId}`,
            );
          }

          continue;
        }

        const manualNormalSnoozeUntil = Number(
          runtime.manualNormalSnoozeUntil || 0,
        );

        if (manualNormalSnoozeUntil > now) {
          const nextRuntime = buildRuntime({
            status: "manual_normal_snooze",
            ...runtimeCounts,
            allOutsideSince: 0,
            cycleArmed: false,
            manualNormalSnoozeUntil,
            insideOverrideUid: "",
            insideOverrideAt: 0,
            now,
          });

          if (
            runtimeSignature(runtime) !==
            runtimeSignature(nextRuntime)
          ) {
            updates[runtimePath] = nextRuntime;
          }

          continue;
        }

        // Không còn ai đủ điều kiện theo dõi nền:
        // - Nếu nhà chưa armed: không được bắt đầu chu kỳ Auto Away mới.
        // - Nếu Auto Away đã armed thành công: phải giữ nguyên armed.
        //   Việc mất quyền nền/auto-start/miễn tối ưu pin không được
        //   tự hạ Mode về Bình thường.
        // - Chỉ hạ Mode khi có một lần enter/initial_sync thực sự xảy ra
        //   sau khi chu kỳ rời nhà đã bắt đầu.
        const storedRearmBlockedUntil = Number(
          runtime.rearmBlockedUntil || 0,
        );

        const activeRearmBlockedUntil =
          storedRearmBlockedUntil > now
            ? storedRearmBlockedUntil
            : 0;

        if (memberCount === 0) {
          const autoAwayAlreadyArmed =
            securityMode === "armed" &&
            securityModeSource === "auto_away";

          const awayCycleStartedAt = Number(
            runtime.allOutsideSince || 0,
          );

          let confirmedInsideUid = "";
          let confirmedInsideAt = 0;

          if (
            autoAwayAlreadyArmed &&
            awayCycleStartedAt > 0
          ) {
            for (const [memberUid, presenceStatus] of
              memberPresenceByUid.entries()) {
              if (
                !presenceStatus.identityMatches ||
                !presenceStatus.sessionActive ||
                presenceStatus.state !== "inside" ||
                presenceStatus.updatedAt <
                  awayCycleStartedAt ||
                (
                  presenceStatus.event !== "enter" &&
                  presenceStatus.event !== "initial_sync"
                )
              ) {
                continue;
              }

              if (
                presenceStatus.updatedAt >
                confirmedInsideAt
              ) {
                confirmedInsideUid = memberUid;
                confirmedInsideAt =
                  presenceStatus.updatedAt;
              }
            }
          }

          let nextRuntime;

          if (confirmedInsideUid) {
            const storedCandidateUid = String(
              runtime.insideOverrideUid || "",
            ).trim();

            const storedCandidateSince = Number(
              runtime.insideCandidateSince || 0,
            );

            const insideCandidateSince =
              storedCandidateUid === confirmedInsideUid &&
              storedCandidateSince > 0
                ? storedCandidateSince
                : now;

            const insideConfirmed =
              now - insideCandidateSince >=
              AUTO_AWAY_INSIDE_CONFIRM_MS;

            if (insideConfirmed) {
              const rearmBlockedUntil =
                now + AUTO_AWAY_REARM_BLOCK_MS;

              nextRuntime = buildRuntime({
                status: "inside_unmonitored",
                ...runtimeCounts,
                allOutsideSince: 0,
                insideCandidateSince: 0,
                rearmBlockedUntil,
                cycleArmed: false,
                insideOverrideUid:
                  confirmedInsideUid,
                insideOverrideAt:
                  confirmedInsideAt,
                now,
              });

              updates[`${homePath}/securityMode`] =
                "normal";
              updates[`${homePath}/securityModeSource`] =
                null;

              logs.push(
                `🏠 AUTO AWAY UNMONITORED MEMBER RETURNED → NORMAL: ${ownerUid} ${homeId} member=${confirmedInsideUid} rearmBlockedUntil=${rearmBlockedUntil}`,
              );
            } else {
              nextRuntime = buildRuntime({
                status:
                  "confirming_inside_unmonitored",
                ...runtimeCounts,
                allOutsideSince:
                  awayCycleStartedAt,
                insideCandidateSince,
                rearmBlockedUntil:
                  activeRearmBlockedUntil,
                cycleArmed: true,
                insideOverrideUid:
                  confirmedInsideUid,
                insideOverrideAt:
                  confirmedInsideAt,
                now,
              });
            }
          } else if (autoAwayAlreadyArmed) {
            nextRuntime = buildRuntime({
              status:
                "armed_monitoring_unavailable",
              ...runtimeCounts,
              allOutsideSince:
                awayCycleStartedAt,
              insideCandidateSince: 0,
              rearmBlockedUntil:
                activeRearmBlockedUntil,
              cycleArmed: true,
              insideOverrideUid: "",
              insideOverrideAt: 0,
              now,
            });

            if (
              String(runtime.status || "") !==
              "armed_monitoring_unavailable"
            ) {
              logs.push(
                `🛡️ AUTO AWAY ARMED KEPT, MONITORING UNAVAILABLE: ${ownerUid} ${homeId}`,
              );
            }
          } else {
            nextRuntime = buildRuntime({
              status:
                activeRearmBlockedUntil > 0
                  ? "rearm_blocked"
                  : "waiting_monitoring",
              ...runtimeCounts,
              allOutsideSince: 0,
              insideCandidateSince: 0,
              rearmBlockedUntil:
                activeRearmBlockedUntil,
              cycleArmed: false,
              insideOverrideUid: "",
              insideOverrideAt: 0,
              now,
            });
          }

          if (
            runtimeSignature(runtime) !==
            runtimeSignature(nextRuntime)
          ) {
            updates[runtimePath] = nextRuntime;
          }

          continue;
        }

        const eligibleMemberInside =
          insideCount > 0;

        const storedInsideOverrideUid = String(
          runtime.insideOverrideUid || "",
        ).trim();

        const storedInsideOverrideAt = Number(
          runtime.insideOverrideAt || 0,
        );

        const storedOverridePresence =
          storedInsideOverrideUid
            ? memberPresenceByUid.get(
                storedInsideOverrideUid,
              )
            : null;

        // Khi một người bị loại khỏi phép tính BẬT Auto Away
        // thực sự đi vào nhà, giữ Mode Bình thường cho tới khi
        // trạng thái của người đó đổi khỏi inside.
        const storedInsideOverrideActive =
          storedInsideOverrideUid &&
          storedInsideOverrideAt > 0 &&
          storedOverridePresence &&
          storedOverridePresence.identityMatches === true &&
          storedOverridePresence.state === "inside" &&
          storedOverridePresence.updatedAt >=
            storedInsideOverrideAt;

        const awayCycleStartedAt = Number(
          runtime.allOutsideSince || 0,
        );

        let newInsideOverrideUid = "";
        let newInsideOverrideAt = 0;

        // Không dùng dữ liệu inside cũ để chặn Auto Away.
        // Chỉ nhận enter/initial_sync xảy ra sau khi chu kỳ
        // tất cả thành viên đủ điều kiện đã rời nhà bắt đầu.
        if (
          !storedInsideOverrideActive &&
          awayCycleStartedAt > 0
        ) {
          for (const [memberUid, presenceStatus] of
            memberPresenceByUid.entries()) {
            if (
              presenceStatus.eligibleForArming ||
              !presenceStatus.identityMatches ||
              presenceStatus.state !== "inside" ||
              presenceStatus.updatedAt <
                awayCycleStartedAt ||
              (
                presenceStatus.event !== "enter" &&
                presenceStatus.event !== "initial_sync"
              )
            ) {
              continue;
            }

            if (
              presenceStatus.updatedAt >
              newInsideOverrideAt
            ) {
              newInsideOverrideUid = memberUid;
              newInsideOverrideAt =
                presenceStatus.updatedAt;
            }
          }
        }

        const excludedMemberInside =
          storedInsideOverrideActive ||
          newInsideOverrideUid !== "";

        const anyInsideForNormalMode =
          eligibleMemberInside ||
          excludedMemberInside;

        const allOutside =
          outsideCount === memberCount;

        const autoAwayAlreadyArmed =
          securityMode === "armed" &&
          securityModeSource === "auto_away";

        let nextRuntime;

        if (anyInsideForNormalMode) {
          const activeInsideOverrideUid =
            storedInsideOverrideActive
              ? storedInsideOverrideUid
              : newInsideOverrideUid;

          const activeInsideOverrideAt =
            storedInsideOverrideActive
              ? storedInsideOverrideAt
              : newInsideOverrideAt;

          const insideStatus =
            excludedMemberInside &&
            !eligibleMemberInside
              ? "inside_unmonitored"
              : "inside";

          if (autoAwayAlreadyArmed) {
            const storedInsideCandidateSince = Number(
              runtime.insideCandidateSince || 0,
            );

            const insideCandidateSince =
              storedInsideCandidateSince > 0
                ? storedInsideCandidateSince
                : now;

            const insideConfirmed =
              now - insideCandidateSince >=
              AUTO_AWAY_INSIDE_CONFIRM_MS;

            if (insideConfirmed) {
              const rearmBlockedUntil =
                now + AUTO_AWAY_REARM_BLOCK_MS;

              nextRuntime = buildRuntime({
                status: insideStatus,
                ...runtimeCounts,
                allOutsideSince: 0,
                insideCandidateSince: 0,
                rearmBlockedUntil,
                cycleArmed: false,
                insideOverrideUid:
                  activeInsideOverrideUid,
                insideOverrideAt:
                  activeInsideOverrideAt,
                now,
              });

              updates[`${homePath}/securityMode`] =
                "normal";
              updates[`${homePath}/securityModeSource`] =
                null;

              logs.push(
                excludedMemberInside &&
                !eligibleMemberInside
                  ? `🏠 AUTO AWAY UNMONITORED MEMBER RETURNED → NORMAL: ${ownerUid} ${homeId} member=${activeInsideOverrideUid} rearmBlockedUntil=${rearmBlockedUntil}`
                  : `🏠 AUTO AWAY MEMBER RETURNED → NORMAL: ${ownerUid} ${homeId} rearmBlockedUntil=${rearmBlockedUntil}`,
              );
            } else {
              nextRuntime = buildRuntime({
                status:
                  excludedMemberInside &&
                  !eligibleMemberInside
                    ? "confirming_inside_unmonitored"
                    : "confirming_inside",
                ...runtimeCounts,
                allOutsideSince:
                  awayCycleStartedAt,
                insideCandidateSince,
                rearmBlockedUntil:
                  activeRearmBlockedUntil,
                cycleArmed: true,
                insideOverrideUid:
                  activeInsideOverrideUid,
                insideOverrideAt:
                  activeInsideOverrideAt,
                now,
              });
            }
          } else {
            nextRuntime = buildRuntime({
              status: insideStatus,
              ...runtimeCounts,
              allOutsideSince: 0,
              insideCandidateSince: 0,
              rearmBlockedUntil:
                activeRearmBlockedUntil,
              cycleArmed: false,
              insideOverrideUid:
                activeInsideOverrideUid,
              insideOverrideAt:
                activeInsideOverrideAt,
              now,
            });
          }
        } else if (!allOutside) {
          // Có trạng thái unknown hoặc dữ liệu chưa đầy đủ:
          // hủy cả hai bộ đếm xác nhận.
          nextRuntime = buildRuntime({
            status:
              activeRearmBlockedUntil > 0
                ? "rearm_blocked"
                : "waiting_presence",
            ...runtimeCounts,
            allOutsideSince: 0,
            insideCandidateSince: 0,
            rearmBlockedUntil:
              activeRearmBlockedUntil,
            cycleArmed: false,
            insideOverrideUid: "",
            insideOverrideAt: 0,
            now,
          });
        } else if (activeRearmBlockedUntil > 0) {
          // Sau khi có người về nhà, Auto Away không được bật lại
          // trong 3 phút dù GPS tạm báo outside.
          nextRuntime = buildRuntime({
            status: "rearm_blocked",
            ...runtimeCounts,
            allOutsideSince: 0,
            insideCandidateSince: 0,
            rearmBlockedUntil:
              activeRearmBlockedUntil,
            cycleArmed: false,
            insideOverrideUid: "",
            insideOverrideAt: 0,
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

            if (securityMode !== "armed") {
              updates[`${homePath}/securityMode`] = "armed";
              updates[`${homePath}/securityModeSource`] = "auto_away";

              logs.push(
                `🛡️ AUTO AWAY ARMED: ${ownerUid} ${homeId} eligible=${memberCount} excluded=${excludedCount}`,
              );
            } else {
              logs.push(
                `🛡️ AUTO AWAY CYCLE READY, MODE ALREADY ARMED: ${ownerUid} ${homeId}`,
              );
            }
          }

          nextRuntime = buildRuntime({
            status: cycleArmed ? "armed" : "countdown",
            ...runtimeCounts,
            allOutsideSince,
            insideCandidateSince: 0,
            rearmBlockedUntil: 0,
            cycleArmed,
            insideOverrideUid: "",
            insideOverrideAt: 0,
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
    `sessionStale=${Math.round(ACCOUNT_SESSION_STALE_MS / 60000)}m`,
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
async function migrateLegacyChatUnreadCounters() {
  const markerRef = db.ref(
    "system/migrations/chatUnreadCounterV1",
  );

  const markerSnap = await markerRef.once("value");
  const marker = asObject(markerSnap.val());

  if (marker.completed === true) {
    return;
  }

  const chatsSnap = await db
    .ref("homeChats")
    .once("value");

  const accounts = asObject(getCachedAccountsObject());
  const chats = asObject(chatsSnap.val());
  const sharedByHome = asObject(
    getCachedSharedByHomeObject(),
  );
  const homeOwners = new Map();

  for (const [ownerUid, rawAccount] of Object.entries(accounts)) {
    const account = asObject(rawAccount);
    const ownedHomes = asObject(account.homes);

    for (const homeId of Object.keys(ownedHomes)) {
      homeOwners.set(homeId, ownerUid);
    }
  }

  const updates = {};
  const now = Date.now();
  let migratedHomes = 0;
  let migratedCounters = 0;

  for (const [homeId, rawChat] of Object.entries(chats)) {
    const ownerUid = String(
      homeOwners.get(homeId) || "",
    ).trim();

    if (!ownerUid) {
      continue;
    }

    const chat = asObject(rawChat);
    const lastReadMap = asObject(chat.lastRead);
    const messages = Object.entries(
      asObject(chat.messages),
    ).map(([messageId, rawMessage]) => {
      const message = asObject(rawMessage);

      return {
        messageId,
        senderUid: String(message.uid || "").trim(),
        time: Number(message.time || 0),
      };
    }).filter((message) => {
      return (
        message.senderUid &&
        Number.isFinite(message.time) &&
        message.time > 0
      );
    });

    const migratedThroughAt = messages.reduce(
      (latest, message) => Math.max(latest, message.time),
      0,
    );

    const recipients = new Set([ownerUid]);
    const sharedMembers = asObject(sharedByHome[homeId]);

    for (const sharedUid of Object.keys(sharedMembers)) {
      const cleanUid = String(sharedUid || "").trim();

      if (cleanUid) {
        recipients.add(cleanUid);
      }
    }

    for (const receiverUid of recipients) {
      const lastReadAt = Number(
        lastReadMap[receiverUid] || 0,
      );

      let count = 0;
      let lastMessageAt = 0;
      let lastMessageId = "";

      for (const message of messages) {
        if (
          message.senderUid === receiverUid ||
          message.time <= lastReadAt
        ) {
          continue;
        }

        count++;

        if (message.time >= lastMessageAt) {
          lastMessageAt = message.time;
          lastMessageId = message.messageId;
        }
      }

      updates[
        `accounts/${receiverUid}/chatUnread/${homeId}`
      ] = {
        count,
        lastReadAt:
          Number.isFinite(lastReadAt) && lastReadAt > 0
            ? lastReadAt
            : 0,
        lastMessageAt,
        lastMessageId,
        lastIncrementedMessageId: "",
        migratedThroughAt,
        updatedAt: now,
      };

      migratedCounters++;
    }

    migratedHomes++;
  }

  updates["system/migrations/chatUnreadCounterV1"] = {
    completed: true,
    completedAt: now,
    migratedHomes,
    migratedCounters,
  };

  await db.ref().update(updates);

  console.log(
    "💬 CHAT UNREAD MIGRATION COMPLETED:",
    `homes=${migratedHomes}`,
    `counters=${migratedCounters}`,
  );
}

function ensureChatUnreadCounterMigration() {
  if (!chatUnreadMigrationPromise) {
    chatUnreadMigrationPromise =
      migrateLegacyChatUnreadCounters().catch((error) => {
        chatUnreadMigrationPromise = null;

        console.log(
          "CHAT UNREAD MIGRATION ERROR:",
          error.message,
        );
      });
  }

  return chatUnreadMigrationPromise;
}

async function incrementChatUnreadCounter({
  receiverUid,
  homeId,
  messageId,
  messageTime,
}) {
  await ensureChatUnreadCounterMigration();

  const cleanMessageId = String(messageId || "").trim();
  const normalizedMessageTime = Number(messageTime || 0);

  if (
    !receiverUid ||
    !homeId ||
    !cleanMessageId ||
    !Number.isFinite(normalizedMessageTime) ||
    normalizedMessageTime <= 0
  ) {
    return 0;
  }

  const counterRef = db.ref(
    `accounts/${receiverUid}/chatUnread/${homeId}`,
  );

  let incremented = false;

  const result = await counterRef.transaction(
    (rawCurrent) => {
      incremented = false;

      const current = rawCurrent &&
        typeof rawCurrent === "object"
          ? rawCurrent
          : {};

      const currentCount = Number(
        typeof rawCurrent === "number"
          ? rawCurrent
          : current.count || 0,
      );

      const lastReadAt = Number(
        current.lastReadAt || 0,
      );

      const migratedThroughAt = Number(
        current.migratedThroughAt || 0,
      );

      const lastIncrementedMessageId = String(
        current.lastIncrementedMessageId || "",
      );

      if (
        cleanMessageId === lastIncrementedMessageId ||
        normalizedMessageTime <= lastReadAt ||
        normalizedMessageTime <= migratedThroughAt
      ) {
        return current;
      }

      incremented = true;

      return {
        ...current,
        count: Math.min(
          9999,
          Number.isFinite(currentCount) && currentCount > 0
            ? Math.floor(currentCount) + 1
            : 1,
        ),
        lastMessageAt: Math.max(
          Number(current.lastMessageAt || 0),
          normalizedMessageTime,
        ),
        lastMessageId: cleanMessageId,
        lastIncrementedMessageId: cleanMessageId,
        updatedAt: Date.now(),
      };
    },
  );

  if (!result.committed || !incremented) {
    return 0;
  }

  const counter = asObject(result.snapshot.val());
  const count = Number(counter.count || 0);

  return Number.isFinite(count) && count > 0
    ? Math.floor(count)
    : 0;
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
  await startBackendDataCache();
  await db.ref("pair_requests").remove();
  console.log("🧹 OLD PAIR REQUESTS CLEARED");

  await cleanupLegacySecurityScheduleState();

  // Chuyển số tin chưa đọc cũ sang counter nhỏ theo từng tài khoản/nhà.
  // Marker đảm bảo chỉ quét lịch sử Home Chat đúng một lần.
  await ensureChatUnreadCounterMigration();

  // Khôi phục các sự cố Alarm chưa được xử lý sau khi backend restart.
  await resumeActiveAlarmIncidents();

  // Watchdog thưa 60 giây, chỉ dùng cache để dọn incident bị bỏ sót.
  startAlarmIncidentWatchdog();

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
          await incrementChatUnreadCounter({
            receiverUid: targetUid,
            homeId,
            messageId: verifiedChatMessageId,
            messageTime: Number(
              verifiedChatMessage.time || 0,
            ),
          });

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

      queueOrderedListCleanup(
        `home_events:${ownerUid}:${homeId}`,
        eventsRef,
        HOME_EVENT_STORAGE_LIMIT,
      );
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
    const startAt = normalizeTimestamp(req.startAt);
    const endAt = normalizeTimestamp(req.endAt);
    const maxPauseDurationMs = 24 * 60 * 60 * 1000;

    if (
      !isValidHHMM(start) ||
      !isValidHHMM(end) ||
      start === end ||
      reason.length > 120 ||
      startAt <= 0 ||
      endAt <= startAt ||
      endAt - startAt > maxPauseDurationMs ||
      startAt < now - 2 * 60 * 1000 ||
      date !== getDateKeyFromTimestamp(startAt)
    ) {
      await reject("INVALID PAUSE DATA");
      return;
    }

    if (
      !doesPauseOverlapEnabledAlarm(
        home,
        startAt,
        endAt,
      )
    ) {
      await reject("OUTSIDE ALARM RANGE");
      return;
    }

    const trustedHomeName =
      String(home.name || "").trim() || homeId;

    const pauseData = {
      date,
      start,
      end,
      startAt,
      endAt,
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

      if (!incident) {
        await reject("INCIDENT NOT FOUND");
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

      // Mỗi tài khoản chỉ được xác nhận/tắt incident nằm trong
      // chính tài khoản đó. Một thành viên không được đóng Alarm của
      // thành viên khác trong cùng nhà.
      if (requestedBy !== receiverUid) {
        await reject("NO PERMISSION");
        return;
      }

      // "Kiểm tra nhà" chỉ là mở ứng dụng để xem tình trạng.
      // Tuyệt đối không đóng incident khi cảm biến vẫn đang nguy hiểm.
      if (action === "check_home" && incident.status === "active") {
        await db
          .ref(
            `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
          )
          .update({
            lastCheckedAt: now,
            lastCheckedBy: requestedBy,
            updatedAt: now,
          });

        await snap.ref.remove();

        console.log(
          "👀 ALARM INCIDENT CHECKED:",
          requestId,
          receiverUid,
          incidentId,
        );

        return;
      }

      // Idempotent: nếu thiết bị khác của cùng tài khoản hoặc một
      // request trước đó đã xử lý incident này, lần bấm sau vẫn được
      // coi là thành công và chỉ đóng Alarm của tài khoản hiện tại.
      if (incident.status !== "active") {
        await sendAlarmResolvedPush({
          uid: receiverUid,
          incidentId,
          homeId,
          resolvedBy: String(
            incident.resolvedBy || requestedBy,
          ),
          action: String(
            incident.resolutionAction ||
            incident.status ||
            "already_resolved",
          ),
        });

        await snap.ref.remove();

        console.log(
          "✅ ALARM ACTION ALREADY RESOLVED:",
          requestId,
          receiverUid,
          incidentId,
          incident.status,
        );

        return;
      }

      await resolveAlarmIncidentForReceiver({
        receiverUid,
        incidentId,
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
                repeatMinutes: 0,
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

    const incidentStateFields = [
      "contact",
      "tamper",
      "occupancy",
      "motion",
      "presence",
      "lock",
      "lock_state",
      "state",
    ];

    const incidentStateChanged =
      incidentStateFields.some((field) => {
        return (
          updateData[field] !== undefined &&
          updateData[field] !== oldData[field]
        );
      });

    if (incidentStateChanged) {
      const latestHomeData = {
        ...homeData,
        devices: {
          ...(homeData.devices || {}),
          [deviceId]: {
            ...oldData,
            ...updateData,
          },
        },
      };

      await validateSecurityIncidentsForHome(
        uid,
        homeId,
        "device_state_changed",
        { homeOverride: latestHomeData },
      );
    }


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