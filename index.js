// 🔥 CORE
const fs = require("fs");
const mqtt = require("mqtt");
const admin = require("firebase-admin");
const crypto = require("crypto");
const path = require("path");
const {
  normalizeLanguageCode,
  localizeBackendText,
  localizeAlarmItemsJson,
} = require("./backend_localizations");

const lastHeartbeatMap = {};
const lastAlarmMap = {};
const lastNotificationMap = {};
const lastScheduleAlarmMap = {};
const pendingEventAlarmMap = {};
const pendingEventAlarmTimerMap = {};
const pendingScheduleReminderMap = {};
const pendingScheduleReminderTimerMap = {};

// ================= OFFLINE-FIRST RUNTIME =================
// Lưu snapshot cấu hình và hàng đợi sự cố ngay cạnh backend để Hub vẫn
// quyết định Alarm/còi khi Internet hoặc Firebase tạm thời mất kết nối.
const LOCAL_RUNTIME_DIR = process.env.SAFEHOME_RUNTIME_DIR ||
  path.join(__dirname, ".safehome_runtime");
const LOCAL_RUNTIME_SNAPSHOT_FILE = path.join(
  LOCAL_RUNTIME_DIR,
  "firebase_snapshot.json",
);
const LOCAL_OFFLINE_QUEUE_FILE = path.join(
  LOCAL_RUNTIME_DIR,
  "offline_queue.json",
);
const LOCAL_RUNTIME_SNAPSHOT_VERSION = 1;
const OFFLINE_QUEUE_MAX_ITEMS = 1000;
const OFFLINE_QUEUE_FLUSH_INTERVAL_MS = 15 * 1000;
const LOCAL_RUNTIME_SNAPSHOT_SAVE_DELAY_MS = 10 * 1000;
const OFFLINE_TRANSIENT_ALARM_TTL_MS = 5 * 60 * 1000;
const offlineOperationQueue = [];
const offlineAlarmDemandMap = new Map();
const offlineAlarmSirenTimerMap = new Map();
const offlineAlarmExpiryTimerMap = new Map();
let firebaseConnected = false;
let localRuntimeSnapshotSaveTimer = null;
let offlineQueueSaveTimer = null;
let offlineQueueFlushTimer = null;
let offlineQueueFlushInProgress = false;
let firebaseConnectionMonitorStarted = false;

// ================= CO SENSOR FIREBASE THROTTLE =================
// Một số cảm biến CO Tuya gửi MQTT gần như liên tục. Trạng thái nguy hiểm
// vẫn phải được ghi/xử lý ngay, còn ppm và telemetry được giới hạn để tránh
// đọc/ghi Firebase hàng trăm nghìn lần mỗi ngày.
const CO_VALUE_PERSIST_INTERVAL_MS = 30 * 1000;
const CO_TELEMETRY_PERSIST_INTERVAL_MS = 60 * 1000;
const coSensorRuntimeMap = new Map();
const coSensorProcessingPromiseMap = new Map();
const vibrationStateClearTimerMap = new Map();

// ================= PHYSICAL HOME SIREN =================
// Còi vật lý là actuator cấp Home, không tự tạo incident. Security chỉ bật
// ở cấp "siren"; Emergency bật ở cấp "fullscreen_siren". Lệnh có thời hạn
// 30 phút để còi tự dừng an toàn nếu backend mất kết nối, sau đó backend sẽ
// làm mới định kỳ khi sự cố vẫn còn active.
const HOME_SIREN_DEFAULT_VOLUME = "high";
const HOME_SIREN_DEFAULT_MELODY = "1";
const HOME_SIREN_COMMAND_DURATION_SEC = 30 * 60;
const HOME_SIREN_REFRESH_INTERVAL_MS = 20 * 60 * 1000;
const HOME_SIREN_RECONCILE_INTERVAL_MS = 15 * 1000;
const HOME_SIREN_STOP_MAX_ATTEMPTS = 3;
const HOME_SIREN_STOP_CONFIRM_WAIT_MS = 1200;
const HOME_SIREN_STOP_RETRY_DELAY_MS = 350;
const HOME_SIREN_ACTION_RESULT_TTL_MS = 30 * 1000;
const VIBRATION_ACTIVE_WINDOW_MS = 15 * 1000;
const homeSirenRuntimeMap = new Map();
// Ghi nhớ trạng thái tắt còi chủ động ngay trong runtime để vòng reconcile
// 15 giây không bật lại còi trước khi accountCache nhận bản ghi Firebase mới.
// Giá trị null là tombstone: Home hiện không còn mute chủ động.
const homeSirenManualMuteRuntimeMap = new Map();
let homeSirenReconcileTimer = null;

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

// Hub ghi heartbeat lên Firebase mỗi 60 giây.
// Backend và app chỉ coi Hub Offline khi quá 180 giây để chịu được
// một chu kỳ ghi chậm mà không tạo cảnh báo giả.
const HUB_HEARTBEAT_INTERVAL_MS = 60 * 1000;
const HUB_HEARTBEAT_STARTED_AT = Date.now();

// ================= SYSTEM HEALTH =================
// Pin yếu, cảm biến/Hub/MQTT mất kết nối chỉ là system_warning.
// Nhóm này không bao giờ tạo Alarm incident, fullscreen hoặc bật còi.
const SYSTEM_HEALTH_CHECK_INTERVAL_MS = 60 * 1000;
const SYSTEM_HEALTH_HUB_TIMEOUT_MS = 180 * 1000;
const SYSTEM_HEALTH_HUB_STARTUP_GRACE_MS = 90 * 1000;
const SYSTEM_HEALTH_NO_DATA_GRACE_MS = 10 * 60 * 1000;
const SYSTEM_HEALTH_STARTED_AT = Date.now();
const systemHealthRuntimeSignatureMap = new Map();
let systemHealthMonitorTimer = null;
let systemHealthCheckInProgress = false;

const alarmIncidentTimerMap = {};
const alarmIncidentAdvanceInProgress = new Set();
const alarmIncidentActionInProgress = new Set();
const homeSirenActionInProgress = new Set();
const alarmIncidentValidationPromiseMap = new Map();
const alarmIncidentStartPromiseMap = new Map();
const alarmIncidentQueuedStageMap = new Map();
const alarmIncidentStageRetryCountMap = new Map();
const localActiveAlarmIncidentMap = new Map();
const securityModeRepeatInProgress = new Set();
// Chặn các packet MQTT trùng/đến song song trước khi chúng kịp tạo hoặc
// cập nhật incident. Map này chỉ tồn tại trong runtime và không thay đổi
// trạng thái thật của cảm biến trong Firebase.
const sensorAlarmEventDebounceMap = new Map();
const SENSOR_ALARM_DEBOUNCE_MAX_AGE_MS = 5 * 60 * 1000;
const SENSOR_ALARM_DEBOUNCE_MAX_ENTRIES = 5000;
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
  if (type === "siren") return 1 * 60 * 60 * 1000;
  if (type === "smoke") return 24 * 60 * 60 * 1000;
  if (type === "sos") return 6 * 60 * 60 * 1000;

  return 6 * 60 * 60 * 1000;
}

function parseSystemHealthTimestamp(value) {
  if (value === null || value === undefined || value === "") {
    return 0;
  }

  const numeric = Number(value);

  if (Number.isFinite(numeric) && numeric > 0) {
    return numeric;
  }

  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function normalizeSystemHealthAvailability(value) {
  const raw =
    value && typeof value === "object"
      ? value.state ?? value.status ?? value.value
      : value;

  return String(raw || "")
    .trim()
    .toLowerCase();
}

function isSystemHealthExplicitlyOffline(value) {
  const availability = normalizeSystemHealthAvailability(value);

  return (
    availability === "offline" ||
    availability === "unavailable" ||
    availability === "disconnected" ||
    availability === "not_available"
  );
}

function isSystemHealthExplicitlyOnline(value) {
  const availability = normalizeSystemHealthAvailability(value);

  return (
    availability === "online" ||
    availability === "available" ||
    availability === "connected"
  );
}

function isProtectionRelevantDeviceType(type) {
  return (
    isSecurityDeviceType(type) ||
    isEmergencyDeviceType(type) ||
    type === "siren" ||
    type === "repeater" ||
    type === "ups"
  );
}

function evaluateDeviceSystemHealth(
  deviceId,
  rawDevice,
  now = Date.now(),
) {
  const device = rawDevice || {};
  const type = String(device.type || "unknown")
    .trim()
    .toLowerCase();
  const deviceName = String(device.name || deviceId).trim() || deviceId;
  const issues = [];
  const availability = device.availability;
  const lastSeen = parseSystemHealthTimestamp(device.last_seen);
  const heartbeatLimitMs = getHeartbeatLimitMs(type);

  let offline = isSystemHealthExplicitlyOffline(availability);

  if (!offline && lastSeen > 0) {
    offline = now - lastSeen > heartbeatLimitMs * 1.3;
  }

  if (
    !offline &&
    lastSeen <= 0 &&
    !isSystemHealthExplicitlyOnline(availability) &&
    now - SYSTEM_HEALTH_STARTED_AT >= SYSTEM_HEALTH_NO_DATA_GRACE_MS
  ) {
    offline = true;
  }

  if (offline) {
    issues.push({
      code: "device_offline",
      level: "warning",
      entityType: "device",
      entityId: deviceId,
      deviceId,
      deviceName,
      deviceType: type,
      message: `${deviceName}: mất kết nối`,
      protectionRelevant: isProtectionRelevantDeviceType(type),
    });
  }

  const batteryValue = Number(device.battery);
  const batteryLow =
    device.battery_low === true ||
    (
      Number.isFinite(batteryValue) &&
      batteryValue >= 0 &&
      batteryValue <= 20
    );

  if (batteryLow) {
    issues.push({
      code: "device_low_battery",
      level: "warning",
      entityType: "device",
      entityId: deviceId,
      deviceId,
      deviceName,
      deviceType: type,
      battery: Number.isFinite(batteryValue)
        ? batteryValue
        : null,
      message: `${deviceName}: pin yếu`,
      protectionRelevant: isProtectionRelevantDeviceType(type),
    });
  }

  return issues;
}

function evaluateHomeSystemHealth(rawHome, now = Date.now()) {
  const home = rawHome || {};
  const issues = [];
  const hubId = String(home.hubId || "").trim();
  const hubStatus =
    home.hubStatus && typeof home.hubStatus === "object"
      ? home.hubStatus
      : {};
  const hubHeartbeatAt = parseSystemHealthTimestamp(
    hubStatus.lastHeartbeatAt,
  );

  let hubTracked = hubId.length > 0;
  let hubOnline = true;
  let mqttOnline = true;

  if (hubTracked) {
    const inStartupGrace =
      now - SYSTEM_HEALTH_STARTED_AT <
      SYSTEM_HEALTH_HUB_STARTUP_GRACE_MS;

    const heartbeatMissingOrStale =
      hubHeartbeatAt <= 0 ||
      now - hubHeartbeatAt > SYSTEM_HEALTH_HUB_TIMEOUT_MS;

    if (!inStartupGrace && heartbeatMissingOrStale) {
      hubOnline = false;
      mqttOnline = false;
      issues.push({
        code: "hub_offline",
        level: "warning",
        entityType: "hub",
        entityId: hubId,
        hubId,
        message: "Hub mất kết nối",
        protectionRelevant: true,
      });
    } else if (
      !heartbeatMissingOrStale &&
      hubStatus.mqttConnected === false
    ) {
      mqttOnline = false;
      issues.push({
        code: "mqtt_offline",
        level: "warning",
        entityType: "hub",
        entityId: hubId,
        hubId,
        message: "MQTT mất kết nối",
        protectionRelevant: true,
      });
    }
  }

  const devices =
    home.devices && typeof home.devices === "object"
      ? home.devices
      : {};

  for (const [deviceId, device] of Object.entries(devices)) {
    issues.push(
      ...evaluateDeviceSystemHealth(deviceId, device, now),
    );
  }

  issues.sort((first, second) => {
    return `${first.code}|${first.entityId}`.localeCompare(
      `${second.code}|${second.entityId}`,
    );
  });

  const protectionComplete = !issues.some(
    (issue) => issue.protectionRelevant === true,
  );
  const issueSignature = issues
    .map((issue) => `${issue.code}:${issue.entityId}`)
    .join("|");
  const status = issues.length > 0 ? "warning" : "ok";

  return {
    status,
    eventCategory: "system_warning",
    alarmLevel: status === "warning" ? "warning" : "info",
    protectionComplete,
    warningCount: issues.length,
    hubTracked,
    hubOnline,
    mqttOnline,
    issues,
    issueSignature,
  };
}

function getHomeNotificationSafety(home) {
  const devices = home.devices || {};
  const unsafeDevices = [];
  const dangerIssues = [];
  const systemWarnings = [];
  const systemHealth = evaluateHomeSystemHealth(home);

  for (const issue of systemHealth.issues) {
    if (issue.message) {
      systemWarnings.push(issue.message);
    }
  }

  for (const [deviceId, device] of Object.entries(devices)) {
    const name = device.name || deviceId;
    const type = device.type || "door";
    const issues = [];

    if (type === "door" || type === "window" || type === "gate") {
      if (device.contact === false) issues.push("đang mở");
      if (device.tamper === true) issues.push("bị tháo");
    }

    if (type === "door_lock" || type === "lock") {
      if (normalizeLockState(device) === "unlocked") {
        issues.push("khóa đang mở");
      }
      if (device.tamper === true) issues.push("bị tháo");
    }

    if (type === "smoke") {
      if (device.smoke === true) issues.push("phát hiện khói");
      if (device.tamper === true) issues.push("bị tháo");
    }

    if (type === "carbon_monoxide") {
      if (
        isActiveSignal(device.carbon_monoxide) ||
        isActiveSignal(device.co_alarm)
      ) {
        issues.push("phát hiện khí CO");
      }
    }

    if (type === "gas") {
      if (
        isActiveSignal(device.gas) ||
        isActiveSignal(device.gas_alarm)
      ) {
        issues.push("rò rỉ gas");
      }
    }

    if (type === "water_leak" || type === "flood") {
      if (
        isActiveSignal(device.water_leak) ||
        isActiveSignal(device.leak) ||
        isActiveSignal(device.water)
      ) {
        issues.push("phát hiện ngập nước");
      }
    }

    if (type === "sos") {
      const lastTriggered = Number(device.last_triggered || 0);
      const isRecentlyTriggered =
        lastTriggered > 0 && Date.now() - lastTriggered < 60 * 1000;

      if (isRecentlyTriggered) issues.push("đã kích hoạt SOS");
    }

    if (issues.length > 0) {
      const detail = `${name}: ${issues.join(", ")}`;
      dangerIssues.push(detail);
      unsafeDevices.push(detail);
    }
  }

  for (const warning of systemWarnings) {
    if (!unsafeDevices.includes(warning)) {
      unsafeDevices.push(warning);
    }
  }

  return {
    safe: dangerIssues.length === 0 && systemWarnings.length === 0,
    protectionComplete: systemHealth.protectionComplete,
    dangerIssues,
    systemWarnings,
    unsafeDevices,
  };
}

function getSystemHealthRuntimeKey(ownerUid, homeId) {
  return `${ownerUid}|${homeId}`;
}

async function checkSystemHealth() {
  if (!firebaseConnected || systemHealthCheckInProgress) {
    return;
  }

  systemHealthCheckInProgress = true;

  try {
    const now = Date.now();
    const updates = {};
    let changedHomes = 0;

    for (const [ownerUid, account] of accountCache.entries()) {
      const homes = account?.homes || {};

      for (const [homeId, rawHome] of Object.entries(homes)) {
        const home = rawHome || {};
        const health = evaluateHomeSystemHealth(home, now);
        const runtimeKey = getSystemHealthRuntimeKey(ownerUid, homeId);
        const signature = [
          health.status,
          health.protectionComplete ? "complete" : "incomplete",
          health.issueSignature,
        ].join("|");
        const current =
          home.systemHealth && typeof home.systemHealth === "object"
            ? home.systemHealth
            : {};
        const currentSignature = [
          String(current.status || ""),
          current.protectionComplete === true
            ? "complete"
            : "incomplete",
          String(current.issueSignature || ""),
        ].join("|");

        if (
          systemHealthRuntimeSignatureMap.get(runtimeKey) === signature ||
          currentSignature === signature
        ) {
          systemHealthRuntimeSignatureMap.set(runtimeKey, signature);
          continue;
        }

        systemHealthRuntimeSignatureMap.set(runtimeKey, signature);
        updates[
          `accounts/${ownerUid}/homes/${homeId}/systemHealth`
        ] = {
          ...health,
          evaluatedAt: now,
        };
        changedHomes++;
      }
    }

    if (changedHomes > 0) {
      await db.ref().update(updates);
      console.log(
        "🩺 SYSTEM HEALTH UPDATED:",
        `homes=${changedHomes}`,
      );
    }
  } catch (error) {
    console.log("SYSTEM HEALTH CHECK ERROR:", error.message);
  } finally {
    systemHealthCheckInProgress = false;
  }
}

function startSystemHealthMonitor() {
  if (systemHealthMonitorTimer) {
    return;
  }

  void checkSystemHealth();

  systemHealthMonitorTimer = setInterval(() => {
    void checkSystemHealth();
  }, SYSTEM_HEALTH_CHECK_INTERVAL_MS);

  console.log(
    "🩺 SYSTEM HEALTH MONITOR STARTED:",
    `interval=${SYSTEM_HEALTH_CHECK_INTERVAL_MS / 1000}s`,
  );
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

function normalizeDeviceAction(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function isVibrationAction(value) {
  const action = normalizeDeviceAction(value);

  return (
    action === "vibration" ||
    action === "shock" ||
    action === "tilt" ||
    action === "drop" ||
    action === "vibrate" ||
    action === "vibration_detected"
  );
}

function isGlassBreakAction(value) {
  const action = normalizeDeviceAction(value);

  return (
    action === "glass_break" ||
    action === "glass_broken" ||
    action === "broken_glass"
  );
}

function waitMs(durationMs) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Number(durationMs) || 0));
  });
}

function getVibrationStateTimerKey(uid, homeId, deviceId) {
  return `${uid}|${homeId}|${deviceId}`;
}

function cancelVibrationStateClear(uid, homeId, deviceId) {
  const timerKey = getVibrationStateTimerKey(uid, homeId, deviceId);
  const timer = vibrationStateClearTimerMap.get(timerKey);

  if (timer) {
    clearTimeout(timer);
    vibrationStateClearTimerMap.delete(timerKey);
  }
}

function scheduleVibrationStateClear(
  uid,
  homeId,
  deviceId,
  eventTime,
) {
  const timerKey = getVibrationStateTimerKey(uid, homeId, deviceId);
  cancelVibrationStateClear(uid, homeId, deviceId);

  const timer = setTimeout(async () => {
    vibrationStateClearTimerMap.delete(timerKey);

    try {
      const deviceRef = db.ref(
        `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
      );
      const snap = await deviceRef.once("value");
      const device = snap.val() || {};

      // Chỉ clear đúng lần rung đã lên lịch. Lần rung mới hơn sẽ có timestamp
      // khác và timer riêng, nên không bị timer cũ ghi đè.
      if (Number(device.last_vibration_at || 0) !== eventTime) {
        return;
      }

      await deviceRef.update({
        vibration: false,
        vibration_active_until: null,
        updated_at: Date.now(),
      });
    } catch (error) {
      console.log(
        "VIBRATION STATE CLEAR ERROR:",
        deviceId,
        error.message,
      );
    }
  }, VIBRATION_ACTIVE_WINDOW_MS + 250);

  vibrationStateClearTimerMap.set(timerKey, timer);
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
    data.co_alarm !== undefined ||
    data.co !== undefined
  ) {
    return "carbon_monoxide";
  }

  if (
    data.alarm !== undefined &&
    (
      data.melody !== undefined ||
      data.duration !== undefined ||
      data.volume !== undefined ||
      data.battpercentage !== undefined
    )
  ) {
    return "siren";
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
    data.vibration_strength !== undefined ||
    data.sensitivity !== undefined
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

  // Bốn thiết bị mới đang dùng thực tế. Giữ nhận diện theo cả IEEE
  // và model để pair lại vẫn đúng nếu người dùng đổi tên friendly_name.
  if (
    id === "0xa4c138ea6ee11777" ||
    model === "dcr-co"
  ) {
    return "carbon_monoxide";
  }

  if (
    id === "0xa4c13839bfd34161" ||
    model === "809wzt"
  ) {
    return "motion";
  }

  if (
    id === "0xa4c138f00d9289fc" ||
    model === "ts0210"
  ) {
    return "vibration";
  }

  if (
    id === "0xa4c1382b53b62852" ||
    model === "nas-ab02b2"
  ) {
    return "siren";
  }

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

function getUserLanguageCode(uid) {
  const cleanUid = String(uid || "").trim();
  const account = cleanUid ? accountCache.get(cleanUid) : null;

  return normalizeLanguageCode(
    account?.languageCode ||
    account?.profile?.languageCode ||
    "vi",
  );
}

function localizePushMessageForUser(uid, rawMessage) {
  const languageCode = getUserLanguageCode(uid);
  const message = JSON.parse(JSON.stringify(rawMessage || {}));

  if (message.data && typeof message.data === "object") {
    if (typeof message.data.title === "string") {
      message.data.title = localizeBackendText(languageCode, message.data.title);
    }
    if (typeof message.data.body === "string") {
      message.data.body = localizeBackendText(languageCode, message.data.body);
    }
    if (typeof message.data.reason === "string") {
      message.data.reason = localizeBackendText(languageCode, message.data.reason);
    }
    if (typeof message.data.alarmItems === "string") {
      message.data.alarmItems = localizeAlarmItemsJson(languageCode, message.data.alarmItems);
    }
    message.data.languageCode = languageCode;
  }

  if (message.notification && typeof message.notification === "object") {
    if (typeof message.notification.title === "string") {
      message.notification.title = localizeBackendText(languageCode, message.notification.title);
    }
    if (typeof message.notification.body === "string") {
      message.notification.body = localizeBackendText(languageCode, message.notification.body);
    }
  }

  const apnsAlert = message?.apns?.payload?.aps?.alert;
  if (typeof apnsAlert === "string") {
    message.apns.payload.aps.alert = localizeBackendText(languageCode, apnsAlert);
  } else if (apnsAlert && typeof apnsAlert === "object") {
    if (typeof apnsAlert.title === "string") {
      apnsAlert.title = localizeBackendText(languageCode, apnsAlert.title);
    }
    if (typeof apnsAlert.body === "string") {
      apnsAlert.body = localizeBackendText(languageCode, apnsAlert.body);
    }
  }

  return message;
}

function ensureLocalRuntimeDirectory() {
  fs.mkdirSync(LOCAL_RUNTIME_DIR, {
    recursive: true,
    mode: 0o700,
  });

  try {
    fs.chmodSync(LOCAL_RUNTIME_DIR, 0o700);
  } catch (_) { }
}

function readLocalJsonFile(filePath, fallbackValue) {
  try {
    if (!fs.existsSync(filePath)) {
      return fallbackValue;
    }

    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    console.log(
      "LOCAL RUNTIME READ ERROR:",
      path.basename(filePath),
      error.message,
    );
    return fallbackValue;
  }
}

function writeLocalJsonFileAtomic(filePath, value) {
  ensureLocalRuntimeDirectory();
  const tempPath = `${filePath}.tmp`;
  fs.writeFileSync(
    tempPath,
    JSON.stringify(value),
    "utf8",
  );
  fs.renameSync(tempPath, filePath);

  try {
    fs.chmodSync(filePath, 0o600);
  } catch (_) { }
}

function buildLocalOfflineAccountSnapshot(rawAccount) {
  const account = rawAccount || {};
  const safeHomes = {};

  for (const [homeId, rawHome] of Object.entries(
    account.homes || {},
  )) {
    const home = rawHome || {};
    const safeDevices = {};

    for (const [deviceId, rawDevice] of Object.entries(
      home.devices || {},
    )) {
      const device = rawDevice || {};
      const {
        notifications: _ignoredNotifications,
        ...safeDevice
      } = device;

      safeDevices[deviceId] = safeDevice;
    }

    safeHomes[homeId] = {
      name: home.name || homeId,
      securityMode: home.securityMode || "normal",
      securityModeRepeatMinutes:
        home.securityModeRepeatMinutes ?? 0,
      alarmPauseToday: home.alarmPauseToday || null,
      devices: safeDevices,
    };
  }

  return {
    homes: safeHomes,
    alarmSettings: account.alarmSettings || {},
    customRules: account.customRules || {},
    sharedHomes: account.sharedHomes || {},
  };
}

function persistLocalRuntimeSnapshotNow() {
  try {
    writeLocalJsonFileAtomic(
      LOCAL_RUNTIME_SNAPSHOT_FILE,
      {
        version: LOCAL_RUNTIME_SNAPSHOT_VERSION,
        savedAt: Date.now(),
        accounts: Object.fromEntries(
          Array.from(accountCache.entries()).map(
            ([uid, account]) => [
              uid,
              buildLocalOfflineAccountSnapshot(account),
            ],
          ),
        ),
        sharedByHome: Object.fromEntries(
          sharedByHomeCache.entries(),
        ),
        deviceMap,
      },
    );
  } catch (error) {
    console.log(
      "LOCAL SNAPSHOT SAVE ERROR:",
      error.message,
    );
  }
}

function scheduleLocalRuntimeSnapshotSave() {
  if (localRuntimeSnapshotSaveTimer) {
    return;
  }

  localRuntimeSnapshotSaveTimer = setTimeout(() => {
    localRuntimeSnapshotSaveTimer = null;
    persistLocalRuntimeSnapshotNow();
  }, LOCAL_RUNTIME_SNAPSHOT_SAVE_DELAY_MS);
}

function persistOfflineQueueNow() {
  try {
    writeLocalJsonFileAtomic(
      LOCAL_OFFLINE_QUEUE_FILE,
      {
        version: 1,
        savedAt: Date.now(),
        operations: offlineOperationQueue,
      },
    );
  } catch (error) {
    console.log(
      "OFFLINE QUEUE SAVE ERROR:",
      error.message,
    );
  }
}

function scheduleOfflineQueueSave() {
  if (offlineQueueSaveTimer) {
    return;
  }

  offlineQueueSaveTimer = setTimeout(() => {
    offlineQueueSaveTimer = null;
    persistOfflineQueueNow();
  }, 250);
}

function loadLocalRuntimeState() {
  ensureLocalRuntimeDirectory();

  const snapshot = readLocalJsonFile(
    LOCAL_RUNTIME_SNAPSHOT_FILE,
    null,
  );

  if (
    snapshot &&
    snapshot.version === LOCAL_RUNTIME_SNAPSHOT_VERSION
  ) {
    for (const [uid, account] of Object.entries(
      snapshot.accounts || {},
    )) {
      accountCache.set(uid, account || {});
    }

    for (const [homeId, members] of Object.entries(
      snapshot.sharedByHome || {},
    )) {
      sharedByHomeCache.set(homeId, members || {});
    }

    for (const [deviceId, map] of Object.entries(
      snapshot.deviceMap || {},
    )) {
      if (map?.uid && map?.homeId) {
        deviceMap[deviceId] = {
          uid: String(map.uid),
          homeId: String(map.homeId),
        };
      }
    }

    console.log(
      "💾 LOCAL FIREBASE SNAPSHOT LOADED:",
      `accounts=${accountCache.size}`,
      `homes=${sharedByHomeCache.size}`,
      `devices=${Object.keys(deviceMap).length}`,
      `savedAt=${Number(snapshot.savedAt || 0)}`,
    );
  }

  const queueData = readLocalJsonFile(
    LOCAL_OFFLINE_QUEUE_FILE,
    null,
  );
  const storedOperations = Array.isArray(
    queueData?.operations,
  )
    ? queueData.operations
    : [];

  offlineOperationQueue.splice(
    0,
    offlineOperationQueue.length,
    ...storedOperations.slice(-OFFLINE_QUEUE_MAX_ITEMS),
  );

  if (offlineOperationQueue.length > 0) {
    console.log(
      "📥 OFFLINE QUEUE LOADED:",
      offlineOperationQueue.length,
    );
  }
}

function applyDeviceUpdateToLocalCache(
  ownerUid,
  homeId,
  deviceId,
  updateData,
) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();
  const cleanDeviceId = String(deviceId || "").trim();

  if (!cleanOwnerUid || !cleanHomeId || !cleanDeviceId) {
    return null;
  }

  const account = accountCache.get(cleanOwnerUid) || {};
  const homes = account.homes || {};
  const home = homes[cleanHomeId] || {};
  const devices = home.devices || {};
  const nextDevice = {
    ...(devices[cleanDeviceId] || {}),
    ...(updateData || {}),
  };
  const nextHome = {
    ...home,
    devices: {
      ...devices,
      [cleanDeviceId]: nextDevice,
    },
  };

  accountCache.set(cleanOwnerUid, {
    ...account,
    homes: {
      ...homes,
      [cleanHomeId]: nextHome,
    },
  });

  scheduleLocalRuntimeSnapshotSave();
  return nextHome;
}

function getOfflineOperationIdentity(operation) {
  if (operation?.type === "firebase_update") {
    return `firebase_update|${String(operation.path || "")}`;
  }

  if (operation?.type === "alarm_item") {
    const itemType = String(
      operation.item?.type || "",
    ).trim();
    const isTransient = [
      "sos",
      "vibration",
      "glass_break",
      "motion",
      "presence",
    ].includes(itemType);
    const timeBucket = isTransient
      ? Math.floor(
          Number(operation.queuedAt || 0) /
          EMERGENCY_MERGE_WINDOW_MS,
        )
      : 0;

    return [
      "alarm_item",
      String(operation.receiverUid || ""),
      getAlarmIncidentItemIdentity(operation.item || {}),
      String(timeBucket),
    ].join("|");
  }

  return String(operation?.id || "");
}

function enqueueOfflineOperation(operation) {
  if (!operation || !operation.type) {
    return;
  }

  const normalized = {
    ...operation,
    id: operation.id || (
      typeof crypto.randomUUID === "function"
        ? crypto.randomUUID()
        : crypto.randomBytes(16).toString("hex")
    ),
    queuedAt: Number(operation.queuedAt || Date.now()),
  };
  const identity = getOfflineOperationIdentity(normalized);
  const existingIndex = offlineOperationQueue.findIndex(
    (item) => getOfflineOperationIdentity(item) === identity,
  );

  if (
    normalized.type === "firebase_update" &&
    existingIndex >= 0
  ) {
    const existing = offlineOperationQueue[existingIndex];
    offlineOperationQueue[existingIndex] = {
      ...existing,
      ...normalized,
      data: {
        ...(existing.data || {}),
        ...(normalized.data || {}),
      },
      queuedAt: existing.queuedAt || normalized.queuedAt,
    };
  } else if (existingIndex < 0) {
    offlineOperationQueue.push(normalized);
  }

  if (offlineOperationQueue.length > OFFLINE_QUEUE_MAX_ITEMS) {
    offlineOperationQueue.splice(
      0,
      offlineOperationQueue.length - OFFLINE_QUEUE_MAX_ITEMS,
    );
  }

  scheduleOfflineQueueSave();
}

function enqueueOfflineFirebaseUpdate(refPath, data) {
  enqueueOfflineOperation({
    type: "firebase_update",
    path: String(refPath || "").trim(),
    data: data || {},
  });
}

function enqueueOfflineAlarmItem(receiverUid, item) {
  enqueueOfflineOperation({
    type: "alarm_item",
    receiverUid: String(receiverUid || "").trim(),
    item,
  });
}

function isQueuedAlarmOperationStillRelevant(operation) {
  const item = operation?.item || {};
  const ownerUid = String(item.ownerUid || "").trim();
  const homeId = String(item.homeId || "").trim();
  const home = getCachedHomeData(ownerUid, homeId);
  const queuedAt = Number(operation?.queuedAt || 0);
  const type = String(item.type || "").trim();

  if (!home) {
    return true;
  }

  if (isPersistentEmergencyIncidentItem(item)) {
    return isEmergencyIncidentItemStillUnsafe(home, item);
  }

  if (
    [
      "sos",
      "vibration",
      "glass_break",
      "motion",
      "presence",
    ].includes(type)
  ) {
    return (
      queuedAt > 0 &&
      Date.now() - queuedAt <=
        OFFLINE_TRANSIENT_ALARM_TTL_MS
    );
  }

  return true;
}

async function flushOfflineOperationQueue() {
  if (
    !firebaseConnected ||
    offlineQueueFlushInProgress ||
    offlineOperationQueue.length === 0
  ) {
    return;
  }

  offlineQueueFlushInProgress = true;
  let completed = 0;

  try {
    while (
      firebaseConnected &&
      offlineOperationQueue.length > 0
    ) {
      const alarmIndex = offlineOperationQueue.findIndex(
        (item) => item?.type === "alarm_item",
      );
      const operationIndex = alarmIndex >= 0
        ? alarmIndex
        : 0;
      const operation = offlineOperationQueue[operationIndex];

      if (operation.type === "firebase_update") {
        if (!operation.path) {
          offlineOperationQueue.splice(operationIndex, 1);
          continue;
        }

        await db
          .ref(operation.path)
          .update(operation.data || {});
      } else if (operation.type === "alarm_item") {
        if (
          !operation.receiverUid ||
          !operation.item ||
          !isQueuedAlarmOperationStillRelevant(operation)
        ) {
          offlineOperationQueue.splice(operationIndex, 1);
          continue;
        }

        await startOrMergeAlarmIncidents(
          operation.receiverUid,
          [operation.item],
        );
      }

      offlineOperationQueue.splice(operationIndex, 1);
      completed++;

      if (completed >= 50) {
        break;
      }
    }
  } catch (error) {
    console.log(
      "OFFLINE QUEUE FLUSH PAUSED:",
      error.message,
    );
  } finally {
    offlineQueueFlushInProgress = false;
    persistOfflineQueueNow();
  }

  if (completed > 0) {
    console.log(
      "📤 OFFLINE QUEUE FLUSHED:",
      completed,
      `remaining=${offlineOperationQueue.length}`,
    );
  }
}

function startOfflineQueueFlushTimer() {
  if (offlineQueueFlushTimer) {
    return;
  }

  offlineQueueFlushTimer = setInterval(() => {
    void flushOfflineOperationQueue();
  }, OFFLINE_QUEUE_FLUSH_INTERVAL_MS);
}

function startFirebaseConnectionMonitor() {
  if (firebaseConnectionMonitorStarted) {
    return;
  }

  firebaseConnectionMonitorStarted = true;

  db.ref(".info/connected").on("value", (snapshot) => {
    const nextConnected = snapshot.val() === true;
    const changed = firebaseConnected !== nextConnected;
    firebaseConnected = nextConnected;

    if (changed) {
      console.log(
        nextConnected
          ? "☁️ FIREBASE CONNECTED"
          : "📴 FIREBASE OFFLINE - LOCAL ALARM MODE",
      );
    }

    if (!nextConnected) {
      void resumeOfflineAlarmDemandsFromSnapshot().catch((error) => {
        console.log(
          "OFFLINE ALARM RESUME ERROR:",
          error.message,
        );
      });
      return;
    }

    scheduleLocalRuntimeSnapshotSave();

    setTimeout(() => {
      void (async () => {
        await flushOfflineOperationQueue();

        try {
          await resumeActiveAlarmIncidents();
        } catch (error) {
          console.log(
            "ALARM RESUME AFTER RECONNECT ERROR:",
            error.message,
          );
        }

        await reconcileAllPhysicalSirens({
          force: true,
          reason: "firebase_reconnected",
        });
      })();
    }, 1000);
  });
}

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
    scheduleLocalRuntimeSnapshotSave();
  };

  const removeDevice = (snap) => {
    const deviceId = String(snap.key || "").trim();

    if (deviceId) {
      delete deviceMap[deviceId];
      scheduleLocalRuntimeSnapshotSave();
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
    scheduleLocalRuntimeSnapshotSave();

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
    scheduleLocalRuntimeSnapshotSave();

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
      scheduleLocalRuntimeSnapshotSave();
    }
  };

  const removeSharedHome = (snap) => {
    const homeId = String(snap.key || "").trim();

    if (homeId) {
      sharedByHomeCache.delete(homeId);
      scheduleLocalRuntimeSnapshotSave();
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
  persistLocalRuntimeSnapshotNow();

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
  const cleanUid = String(uid || "").trim();

  if (!cleanUid) {
    return [];
  }

  const accountSnap = await db
    .ref(`accounts/${cleanUid}`)
    .once("value");

  const account = accountSnap.val() || {};
  const activeSession =
    account.activeSession &&
    typeof account.activeSession === "object"
      ? account.activeSession
      : {};

  const installationId = String(
    activeSession.installationId || "",
  ).trim();
  const sessionId = String(
    activeSession.sessionId || "",
  ).trim();

  if (!installationId || !sessionId) {
    console.log(
      "❌ NO ACTIVE SESSION FOR PUSH:",
      cleanUid,
    );
    return [];
  }

  const session =
    account.sessions?.[installationId] &&
    typeof account.sessions[installationId] === "object"
      ? account.sessions[installationId]
      : null;

  if (
    session &&
    (
      String(session.sessionId || "").trim() !== sessionId ||
      session.signedIn === false
    )
  ) {
    console.log(
      "❌ ACTIVE SESSION RECORD MISMATCH:",
      cleanUid,
      installationId,
    );
    return [];
  }

  const tokenEntry =
    account.fcmTokens?.[installationId];

  if (
    !tokenEntry ||
    typeof tokenEntry !== "object"
  ) {
    console.log(
      "❌ NO ACTIVE INSTALLATION TOKEN:",
      cleanUid,
      installationId,
    );
    return [];
  }

  const tokenSessionId = String(
    tokenEntry.sessionId || "",
  ).trim();
  const token = normalizeFcmToken(tokenEntry.token);

  if (
    !token ||
    tokenSessionId !== sessionId
  ) {
    console.log(
      "❌ ACTIVE TOKEN SESSION MISMATCH:",
      cleanUid,
      installationId,
    );
    return [];
  }

  return [
    {
      token,
      paths: [
        `accounts/${cleanUid}/fcmTokens/${installationId}`,
      ],
    },
  ];
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
  const localizedMessage = localizePushMessageForUser(uid, message);

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
        ...localizedMessage,
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
        : "Nhắc nhở SafeHome";

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
        ? "Nhắc nhở: Nhà đã an toàn"
        : "Nhắc nhở: Cần kiểm tra",
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
    const cachedHome = getCachedHomeData(ownerUid, homeId);
    let pause = cachedHome?.alarmPauseToday;

    if (!cachedHome) {
      const snap = await db
        .ref(`accounts/${ownerUid}/homes/${homeId}/alarmPauseToday`)
        .once("value");

      pause = snap.val();
    }

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
    const respectPause = options?.respectPause !== false;

    // Công tắc Alarm cấp user cũ không còn tham gia quyết định Alarm.
    // Mode nhà là nguồn điều khiển duy nhất; Pause Today chỉ chặn Alarm theo lịch.
    if (respectPause) {
      const paused = await isHomeAlarmPausedToday(ownerUid, homeId);

      if (paused) {
        console.log("⏸️ HOME ALARM PAUSED:", ownerUid, homeId);
        return false;
      }
    }

    return true;
  } catch (err) {
    console.log("ALARM PAUSE CHECK ERROR:", err.message);
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

  eventCategory = "",
  alarmLevel = "",

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
    const languageCode = getUserLanguageCode(uid);
    const localizedTitle = localizeBackendText(
      languageCode,
      String(title || ""),
    );
    const localizedMessage = localizeBackendText(
      languageCode,
      String(message || ""),
    );

    const listRef = db.ref(
      `accounts/${uid}/notifications`,
    );

    const notificationRef = listRef.push();

    await notificationRef.set({
      id: notificationRef.key,
      type,
      category,
      severity,

      // Chuẩn Alarm Engine dùng chung cho backend/Firebase/app.
      // Giữ severity cũ để tương thích với frontend hiện tại.
      eventCategory: String(eventCategory || ""),
      alarmLevel: String(alarmLevel || ""),

      title: localizedTitle,
      message: localizedMessage,
      languageCode,
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
async function sendUnprotectedSensorNotification(
  receiverUid,
  {
    ownerUid,
    homeId,
    homeName,
    deviceId,
    deviceName,
    deviceType,
    reason,
    eventCategory,
  },
) {
  const title = String(homeName || "Nhà").trim() || "Nhà";
  const body = String(reason || "Có sự kiện cảm biến").trim();
  const notificationKey = [
    "unprotected",
    receiverUid,
    homeId,
    deviceId,
    body,
  ].join("|");
  const now = Date.now();

  if (
    lastNotificationMap[notificationKey] &&
    now - lastNotificationMap[notificationKey] < 30 * 1000
  ) {
    return;
  }

  lastNotificationMap[notificationKey] = now;

  await addHomeNotificationFromBackend({
    uid: receiverUid,
    homeId,
    homeName: title,
    type: "sensor_notification",
    title,
    message: body,
    category: "sensor",
    severity: eventCategory === SENSOR_EVENT_CATEGORY.EMERGENCY
      ? "warning"
      : "info",
    eventCategory,
    alarmLevel: "warning",
    entityType: "device",
    entityId: deviceId,
  });

  const data = {
    type: "sensor_notification",
    title,
    body,
    ownerUid: String(ownerUid || ""),
    homeId: String(homeId || ""),
    homeName: title,
    deviceId: String(deviceId || ""),
    deviceName: String(deviceName || ""),
    deviceType: String(deviceType || ""),
    reason: body,
    eventCategory: String(eventCategory || ""),
    alarmLevel: "warning",
    securityMode: "unprotected",
    clickAction: "sensor_notification",
  };

  await sendPushToUser(
    receiverUid,
    {
      data,
      notification: {
        title,
        body,
      },
      android: {
        priority: "high",
        notification: {
          channelId: "safehome_sensor_notification_v1",
          sound: "default",
          tag: `safehome_sensor_${homeId}_${deviceId}`,
        },
      },
      apns: {
        headers: {
          "apns-priority": "10",
        },
        payload: {
          aps: {
            alert: { title, body },
            sound: "default",
            threadId: `safehome_sensor_${homeId}`,
          },
        },
      },
    },
    "UNPROTECTED SENSOR NOTIFICATION",
  );
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

function hasLocalActiveAlarmIncidentForReceiver(receiverUid) {
  const prefix = `${String(receiverUid || "").trim()}|`;

  if (prefix === "|") {
    return false;
  }

  for (const [key, value] of localActiveAlarmIncidentMap.entries()) {
    if (
      key.startsWith(prefix) &&
      value?.incident?.status === "active"
    ) {
      return true;
    }
  }

  return false;
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

function rescheduleAlarmIncidentExpireTimer(
  receiverUid,
  incidentId,
  expireAt,
) {
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
    Math.max(0, Number(expireAt || 0) - Date.now()),
  );

  alarmIncidentTimerMap[key] = timers;
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
  const seenIdentities = new Set();

  for (const rawItem of Array.isArray(items) ? items : []) {
    const item = rawItem || {};
    const homeId = String(item.homeId || "").trim();
    const reason = String(item.reason || "").trim();

    if (!homeId || !reason) {
      continue;
    }

    // Không chỉ so sánh homeId + reason: hai cảm biến cùng tên/lý do trong
    // một nhà vẫn phải được giữ thành hai item độc lập.
    const identity = getAlarmIncidentItemIdentity(item);

    if (!seenIdentities.has(identity)) {
      seenIdentities.add(identity);
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
        eventCategory: String(
          item.eventCategory || "",
        ).trim(),
        alarmLevel: String(
          item.alarmLevel || item.severity || "",
        ).trim(),
        physicalSirenEnabled:
          item.physicalSirenEnabled !== false,
        fullscreenEnabled:
          item.fullscreenEnabled !== false,
        triggerDelaySeconds: Math.min(
          120,
          Math.max(
            0,
            Number.parseInt(
              item.triggerDelaySeconds || 0,
              10,
            ) || 0,
          ),
        ),
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


// Một nguồn duy nhất cho danh sách người nhận Alarm của Home:
// Chủ nhà + các UID đang có trong sharedByHome/{homeId}.
function getAlarmReceiverUidsForHome(ownerUid, homeId) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();

  if (!cleanOwnerUid || !cleanHomeId) {
    return [];
  }

  const receiverUids = new Set([cleanOwnerUid]);
  const sharedMembers =
    sharedByHomeCache.get(cleanHomeId) || {};

  for (const sharedUid of Object.keys(sharedMembers)) {
    const cleanUid = String(sharedUid || "").trim();

    // Không ghi incident vào một UID đã bị xóa khỏi /accounts.
    if (cleanUid && getCachedAccountData(cleanUid)) {
      receiverUids.add(cleanUid);
    }
  }

  return Array.from(receiverUids);
}


function getHomeSirenRuntimeKey(ownerUid, homeId) {
  return `${String(ownerUid || "").trim()}|${String(homeId || "").trim()}`;
}

function getHomeSirenIncidentMuteKey(receiverUid, incidentId) {
  return crypto
    .createHash("sha256")
    .update(
      `${String(receiverUid || "").trim()}|${String(incidentId || "").trim()}`,
    )
    .digest("hex")
    .slice(0, 24);
}

function normalizeHomeSirenManualMute(value) {
  if (!value || value.active !== true) {
    return null;
  }

  const mutedIncidentKeys = {};

  for (const [key, enabled] of Object.entries(
    value.mutedIncidentKeys || {},
  )) {
    const cleanKey = String(key || "").trim();

    if (cleanKey && enabled === true) {
      mutedIncidentKeys[cleanKey] = true;
    }
  }

  if (Object.keys(mutedIncidentKeys).length === 0) {
    return null;
  }

  return {
    active: true,
    mutedAt: Number(value.mutedAt || 0),
    mutedBy: String(value.mutedBy || "").trim(),
    mutedIncidentKeys,
  };
}

async function getHomeSirenManualMute(
  ownerUid,
  homeId,
  { useDatabase = false } = {},
) {
  const runtimeKey = getHomeSirenRuntimeKey(ownerUid, homeId);

  if (homeSirenManualMuteRuntimeMap.has(runtimeKey)) {
    return homeSirenManualMuteRuntimeMap.get(runtimeKey);
  }

  let rawValue = null;
  const cachedHome = getCachedHomeData(ownerUid, homeId);

  if (useDatabase || !cachedHome) {
    const snap = await db
      .ref(
        `accounts/${ownerUid}/homes/${homeId}/sirenManualMute`,
      )
      .once("value");
    rawValue = snap.val();
  } else {
    rawValue = cachedHome.sirenManualMute;
  }

  const normalized = normalizeHomeSirenManualMute(rawValue);
  homeSirenManualMuteRuntimeMap.set(runtimeKey, normalized);
  return normalized;
}

async function clearHomeSirenManualMute(ownerUid, homeId) {
  const runtimeKey = getHomeSirenRuntimeKey(ownerUid, homeId);
  homeSirenManualMuteRuntimeMap.set(runtimeKey, null);

  await db
    .ref(
      `accounts/${ownerUid}/homes/${homeId}/sirenManualMute`,
    )
    .remove();
}

function normalizeHomeSirenVolume(value) {
  const normalized = String(value || "").trim().toLowerCase();

  return ["low", "medium", "high"].includes(normalized)
    ? normalized
    : HOME_SIREN_DEFAULT_VOLUME;
}

function normalizeHomeSirenMelody(value) {
  const melody = Number.parseInt(value, 10);

  return Number.isFinite(melody) && melody >= 1 && melody <= 18
    ? String(melody)
    : HOME_SIREN_DEFAULT_MELODY;
}

function normalizeHomeSirenDuration(value) {
  const duration = Number.parseInt(value, 10);

  return Number.isFinite(duration) && duration >= 1 && duration <= 1800
    ? duration
    : HOME_SIREN_COMMAND_DURATION_SEC;
}

function getHomeSirenDevicesFromHome(home) {
  const devices = home?.devices || {};

  return Object.entries(devices)
    .filter(([, device]) => {
      return String(device?.type || "").trim() === "siren";
    })
    .map(([deviceId, device]) => ({
      deviceId,
      device: device || {},
    }));
}

async function getHomeSirenDevices(ownerUid, homeId) {
  let home = getCachedHomeData(ownerUid, homeId);

  if (!home) {
    try {
      const snap = await db
        .ref(`accounts/${ownerUid}/homes/${homeId}`)
        .once("value");
      home = snap.val() || null;
    } catch (error) {
      console.log(
        "HOME SIREN LOAD HOME ERROR:",
        ownerUid,
        homeId,
        error.message,
      );
    }
  }

  return getHomeSirenDevicesFromHome(home);
}

function publishHomeSirenMqtt(deviceId, payload) {
  return new Promise((resolve) => {
    if (!client.connected) {
      resolve({
        ok: false,
        error: "mqtt_offline",
      });
      return;
    }

    client.publish(
      `zigbee2mqtt/${deviceId}/set`,
      JSON.stringify(payload),
      {
        qos: 1,
        retain: false,
      },
      (error) => {
        resolve({
          ok: !error,
          error: error?.message || "",
        });
      },
    );
  });
}

function getCachedHomeSirenReport(
  ownerUid,
  homeId,
  deviceId,
) {
  const home = getCachedHomeData(ownerUid, homeId);
  const device = home?.devices?.[deviceId];

  if (!device || device.alarm === undefined) {
    return null;
  }

  return {
    alarmOn: isActiveSignal(device.alarm),
    reportedAt: Number(device.last_siren_report_at || 0),
  };
}

async function waitForHomeSirenReportedOff(
  ownerUid,
  homeId,
  deviceId,
  commandStartedAt,
  timeoutMs = HOME_SIREN_STOP_CONFIRM_WAIT_MS,
) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const report = getCachedHomeSirenReport(
      ownerUid,
      homeId,
      deviceId,
    );

    if (
      report &&
      report.alarmOn === false &&
      report.reportedAt >= commandStartedAt
    ) {
      return true;
    }

    await waitMs(120);
  }

  try {
    const snap = await db
      .ref(
        `accounts/${ownerUid}/homes/${homeId}/devices/${deviceId}`,
      )
      .once("value");
    const device = snap.val() || {};

    return (
      device.alarm !== undefined &&
      !isActiveSignal(device.alarm) &&
      Number(device.last_siren_report_at || 0) >= commandStartedAt
    );
  } catch (_) {
    return false;
  }
}

async function setPhysicalSirenForHome(
  ownerUid,
  homeId,
  shouldTurnOn,
  {
    force = false,
    reason = "alarm_incident",
  } = {},
) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();

  if (!cleanOwnerUid || !cleanHomeId) {
    return {
      status: "invalid_home",
      deviceCount: 0,
      successCount: 0,
      confirmedCount: 0,
    };
  }

  const devices = await getHomeSirenDevices(
    cleanOwnerUid,
    cleanHomeId,
  );
  const deviceIds = devices
    .map((item) => item.deviceId)
    .sort();
  const deviceSignature = deviceIds.join(",");
  const runtimeKey = getHomeSirenRuntimeKey(
    cleanOwnerUid,
    cleanHomeId,
  );
  const previous = homeSirenRuntimeMap.get(runtimeKey) || {};
  const now = Date.now();
  const needsRefresh =
    shouldTurnOn &&
    now - Number(previous.lastCommandAt || 0) >=
      HOME_SIREN_REFRESH_INTERVAL_MS;
  const stableStatus = shouldTurnOn
    ? previous.status === "active"
    : previous.status === "stopped";

  if (
    !force &&
    previous.desiredOn === shouldTurnOn &&
    previous.deviceSignature === deviceSignature &&
    stableStatus &&
    !needsRefresh
  ) {
    return {
      status: previous.status,
      deviceCount: deviceIds.length,
      successCount: deviceIds.length,
      confirmedCount: Number(
        previous.confirmedCount ?? deviceIds.length,
      ),
      skipped: true,
    };
  }

  // Đổi desiredOn trước khi publish OFF để packet phản hồi `alarm:false`
  // không bị nhánh tự bật lại coi là còi tự tắt khi incident vẫn active.
  homeSirenRuntimeMap.set(runtimeKey, {
    ...previous,
    desiredOn: shouldTurnOn,
    deviceSignature,
    reason,
    updatedAt: now,
  });

  if (devices.length === 0) {
    homeSirenRuntimeMap.set(runtimeKey, {
      desiredOn: shouldTurnOn,
      deviceSignature,
      status: "no_devices",
      lastCommandAt: 0,
      confirmedCount: 0,
      reason,
      updatedAt: now,
    });

    console.log(
      "📢 HOME SIREN NO DEVICE:",
      cleanOwnerUid,
      cleanHomeId,
      shouldTurnOn ? "ON" : "OFF",
      reason,
    );

    return {
      status: "no_devices",
      deviceCount: 0,
      successCount: 0,
      confirmedCount: 0,
    };
  }

  if (!client.connected) {
    homeSirenRuntimeMap.set(runtimeKey, {
      desiredOn: shouldTurnOn,
      deviceSignature,
      status: "mqtt_offline",
      lastCommandAt: 0,
      confirmedCount: 0,
      reason,
      updatedAt: now,
    });

    console.log(
      "📢 HOME SIREN MQTT OFFLINE:",
      cleanOwnerUid,
      cleanHomeId,
      shouldTurnOn ? "ON" : "OFF",
      reason,
    );

    return {
      status: "mqtt_offline",
      deviceCount: devices.length,
      successCount: 0,
      confirmedCount: 0,
    };
  }

  let successCount = 0;
  let confirmedCount = 0;

  for (const { deviceId, device } of devices) {
    if (shouldTurnOn) {
      const result = await publishHomeSirenMqtt(
        deviceId,
        {
          alarm: true,
          volume: normalizeHomeSirenVolume(
            device.sirenVolume ?? device.volume,
          ),
          melody: normalizeHomeSirenMelody(
            device.sirenMelody ?? device.melody,
          ),
          duration: normalizeHomeSirenDuration(
            device.sirenDuration ?? device.duration,
          ),
        },
      );

      if (result.ok) {
        successCount++;
      } else {
        console.log(
          "HOME SIREN MQTT COMMAND ERROR:",
          deviceId,
          result.error,
        );
      }

      continue;
    }

    let commandAccepted = false;
    let reportedOff = false;

    for (
      let attempt = 1;
      attempt <= HOME_SIREN_STOP_MAX_ATTEMPTS;
      attempt++
    ) {
      const commandStartedAt = Date.now();
      const result = await publishHomeSirenMqtt(
        deviceId,
        { alarm: false },
      );

      if (!result.ok) {
        console.log(
          "HOME SIREN MQTT STOP ERROR:",
          deviceId,
          `attempt=${attempt}`,
          result.error,
        );
      } else {
        commandAccepted = true;
        reportedOff = await waitForHomeSirenReportedOff(
          cleanOwnerUid,
          cleanHomeId,
          deviceId,
          commandStartedAt,
        );

        if (reportedOff) {
          break;
        }
      }

      if (attempt < HOME_SIREN_STOP_MAX_ATTEMPTS) {
        await waitMs(HOME_SIREN_STOP_RETRY_DELAY_MS);
      }
    }

    if (commandAccepted) {
      successCount++;
    }

    if (reportedOff) {
      confirmedCount++;
    }
  }

  const allSucceeded = successCount === devices.length;
  const allConfirmed = confirmedCount === devices.length;
  const status = allSucceeded
    ? shouldTurnOn
      ? "active"
      : allConfirmed
        ? "stopped"
        : "stopped_unconfirmed"
    : "partial";

  homeSirenRuntimeMap.set(runtimeKey, {
    desiredOn: shouldTurnOn,
    deviceSignature,
    status,
    lastCommandAt: allSucceeded ? now : 0,
    confirmedCount,
    reason,
    updatedAt: Date.now(),
  });

  console.log(
    shouldTurnOn
      ? "🚨 HOME SIREN ON:"
      : "🔕 HOME SIREN OFF:",
    cleanOwnerUid,
    cleanHomeId,
    `devices=${successCount}/${devices.length}`,
    `confirmed=${confirmedCount}/${devices.length}`,
    reason,
  );

  return {
    status,
    deviceCount: devices.length,
    successCount,
    confirmedCount,
  };
}

function incidentRequiresPhysicalSiren(incident) {
  if (!incident || incident.status !== "active") {
    return false;
  }

  const requestedStatus = String(
    incident.homeSirenStatus || "",
  ).trim();

  if (
    [
      "start_requested",
      "active",
      "partial",
      "mqtt_offline",
      "no_devices",
    ].includes(requestedStatus)
  ) {
    return true;
  }

  const stage = String(incident.stage || "").trim();

  if (incident.flowType === "emergency") {
    return stage === "fullscreen_siren" || stage === "calling";
  }

  return stage === "siren" || stage === "calling";
}

function collectHomeIncidentKeysInCache(
  ownerUid,
  homeId,
  { requirePhysicalSiren = false } = {},
) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();
  const incidentKeys = new Set();

  for (const [receiverUid, account] of accountCache.entries()) {
    const incidents = account?.alarmIncidents || {};

    for (const [incidentId, incident] of Object.entries(incidents)) {
      const belongsToHome =
        String(incident?.ownerUid || "").trim() === cleanOwnerUid &&
        String(incident?.homeId || "").trim() === cleanHomeId;
      const isActive = incident?.status === "active";

      if (
        belongsToHome &&
        isActive &&
        (!requirePhysicalSiren || incidentRequiresPhysicalSiren(incident))
      ) {
        incidentKeys.add(
          getHomeSirenIncidentMuteKey(receiverUid, incidentId),
        );
      }
    }
  }

  return incidentKeys;
}

function collectPhysicalSirenDemandKeysInCache(ownerUid, homeId) {
  return collectHomeIncidentKeysInCache(
    ownerUid,
    homeId,
    { requirePhysicalSiren: true },
  );
}

async function collectHomeIncidentKeysInDatabase(
  ownerUid,
  homeId,
  { requirePhysicalSiren = false } = {},
) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();
  const receiverUids = getAlarmReceiverUidsForHome(
    cleanOwnerUid,
    cleanHomeId,
  );
  const incidentKeys = new Set();

  const snapshots = await Promise.all(
    receiverUids.map(async (receiverUid) => ({
      receiverUid,
      snap: await db
        .ref(`accounts/${receiverUid}/alarmIncidents`)
        .once("value"),
    })),
  );

  for (const { receiverUid, snap } of snapshots) {
    const incidents = snap.val() || {};

    for (const [incidentId, incident] of Object.entries(incidents)) {
      const belongsToHome =
        String(incident?.ownerUid || "").trim() === cleanOwnerUid &&
        String(incident?.homeId || "").trim() === cleanHomeId;
      const isActive = incident?.status === "active";

      if (
        belongsToHome &&
        isActive &&
        (!requirePhysicalSiren || incidentRequiresPhysicalSiren(incident))
      ) {
        incidentKeys.add(
          getHomeSirenIncidentMuteKey(receiverUid, incidentId),
        );
      }
    }
  }

  return incidentKeys;
}

async function mutePhysicalSirenForHome(
  ownerUid,
  homeId,
  mutedBy,
  { reason = "manual_siren_mute" } = {},
) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();
  const now = Date.now();

  if (!cleanOwnerUid || !cleanHomeId) {
    return {
      status: "invalid_home",
      mutedIncidentCount: 0,
    };
  }

  // Snapshot toàn bộ incident active của Home, kể cả incident chưa tới cấp còi.
  // Nhờ vậy vòng leo thang sau đó cũng không bật lại còi của đúng sự cố đã tắt.
  // Incident mới có ID mới nên vẫn có thể kích hoạt còi trở lại.
  const activeIncidentKeys = collectHomeIncidentKeysInCache(
    cleanOwnerUid,
    cleanHomeId,
  );
  const databaseIncidentKeys = await collectHomeIncidentKeysInDatabase(
    cleanOwnerUid,
    cleanHomeId,
  );

  for (const key of databaseIncidentKeys) {
    activeIncidentKeys.add(key);
  }

  const mutedIncidentKeys = Object.fromEntries(
    Array.from(activeIncidentKeys).map((key) => [key, true]),
  );
  const muteState = activeIncidentKeys.size > 0
    ? {
        active: true,
        mutedAt: now,
        mutedBy: String(mutedBy || "").trim(),
        mutedIncidentKeys,
      }
    : null;
  const runtimeKey = getHomeSirenRuntimeKey(
    cleanOwnerUid,
    cleanHomeId,
  );

  homeSirenManualMuteRuntimeMap.set(runtimeKey, muteState);

  if (muteState) {
    await db
      .ref(
        `accounts/${cleanOwnerUid}/homes/${cleanHomeId}/sirenManualMute`,
      )
      .set(muteState);
  } else {
    await db
      .ref(
        `accounts/${cleanOwnerUid}/homes/${cleanHomeId}/sirenManualMute`,
      )
      .remove();
  }

  const result = await setPhysicalSirenForHome(
    cleanOwnerUid,
    cleanHomeId,
    false,
    {
      force: true,
      reason,
    },
  );

  console.log(
    "🔕 HOME SIREN MANUALLY MUTED:",
    cleanOwnerUid,
    cleanHomeId,
    `incidents=${activeIncidentKeys.size}`,
    `by=${String(mutedBy || "").trim()}`,
  );

  return {
    ...result,
    mutedIncidentCount: activeIncidentKeys.size,
  };
}

async function reconcilePhysicalSirenForHome(
  ownerUid,
  homeId,
  {
    force = false,
    useDatabase = false,
    reason = "siren_reconcile",
  } = {},
) {
  let demandKeys = new Set();

  try {
    demandKeys = useDatabase
      ? await collectHomeIncidentKeysInDatabase(
          ownerUid,
          homeId,
          { requirePhysicalSiren: true },
        )
      : collectPhysicalSirenDemandKeysInCache(
          ownerUid,
          homeId,
        );

    const manualMute = await getHomeSirenManualMute(
      ownerUid,
      homeId,
      { useDatabase },
    );

    if (manualMute && demandKeys.size === 0) {
      // Khi toàn bộ sự cố cũ đã kết thúc, tự bỏ mute để sự cố mới sau này
      // vẫn có thể kích hoạt còi bình thường.
      await clearHomeSirenManualMute(ownerUid, homeId);
    }

    const mutedKeys = new Set(
      Object.keys(manualMute?.mutedIncidentKeys || {}),
    );
    const hasUnmutedDemand = Array.from(demandKeys).some(
      (key) => !mutedKeys.has(key),
    );

    await setPhysicalSirenForHome(
      ownerUid,
      homeId,
      hasUnmutedDemand,
      {
        force,
        reason: manualMute && !hasUnmutedDemand
          ? `${reason}:manual_muted`
          : reason,
      },
    );
  } catch (error) {
    console.log(
      "HOME SIREN RECONCILE READ ERROR:",
      ownerUid,
      homeId,
      error.message,
    );
  }
}

async function requestPhysicalSirenForIncident(
  receiverUid,
  incidentId,
  incident,
  reason,
) {
  const ownerUid = String(
    incident?.ownerUid || receiverUid,
  ).trim();
  const homeId = String(incident?.homeId || "").trim();

  if (!ownerUid || !homeId) {
    return {
      status: "invalid_home",
      deviceCount: 0,
      successCount: 0,
    };
  }

  const incidentRef = db.ref(
    `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
  );
  const now = Date.now();

  // Ghi ý định trước khi publish để backend restart giữa chừng vẫn biết
  // sự cố này đang yêu cầu còi vật lý.
  await incidentRef.update({
    homeSirenStatus: "start_requested",
    homeSirenRequestedAt: now,
    updatedAt: now,
  });

  const manualMute = await getHomeSirenManualMute(
    ownerUid,
    homeId,
    { useDatabase: true },
  );
  const incidentMuteKey = getHomeSirenIncidentMuteKey(
    receiverUid,
    incidentId,
  );
  const isManuallyMuted =
    manualMute?.mutedIncidentKeys?.[incidentMuteKey] === true;

  let result;

  if (isManuallyMuted) {
    // Incident này đã được người dùng chủ động tắt còi. Chạy reconcile để
    // vẫn bật còi nếu Home đồng thời có một incident mới chưa bị mute.
    await reconcilePhysicalSirenForHome(
      ownerUid,
      homeId,
      {
        useDatabase: true,
        reason: `${reason}:manual_muted`,
      },
    );

    const devices = await getHomeSirenDevices(ownerUid, homeId);
    result = {
      status: "manual_muted",
      deviceCount: devices.length,
      successCount: 0,
    };
  } else {
    result = await setPhysicalSirenForHome(
      ownerUid,
      homeId,
      true,
      { reason },
    );
  }

  await incidentRef.update({
    homeSirenStatus: result.status,
    homeSirenDeviceCount: result.deviceCount,
    homeSirenCommandedAt: Date.now(),
    updatedAt: Date.now(),
  });

  return result;
}

function collectCachedHomesWithSiren() {
  const homes = [];

  for (const [ownerUid, account] of accountCache.entries()) {
    for (const [homeId, home] of Object.entries(
      account?.homes || {},
    )) {
      if (getHomeSirenDevicesFromHome(home).length > 0) {
        homes.push({ ownerUid, homeId });
      }
    }
  }

  return homes;
}

async function reconcileAllPhysicalSirens({
  force = false,
  reason = "periodic_reconcile",
} = {}) {
  const homes = collectCachedHomesWithSiren();

  for (const { ownerUid, homeId } of homes) {
    await reconcilePhysicalSirenForHome(
      ownerUid,
      homeId,
      {
        force,
        useDatabase: false,
        reason,
      },
    );
  }
}

function startPhysicalSirenMonitor() {
  if (homeSirenReconcileTimer) {
    return;
  }

  homeSirenReconcileTimer = setInterval(() => {
    void reconcileAllPhysicalSirens({
      reason: "periodic_reconcile",
    });
  }, HOME_SIREN_RECONCILE_INTERVAL_MS);

  console.log(
    "📢 HOME SIREN MONITOR STARTED:",
    `interval=${HOME_SIREN_RECONCILE_INTERVAL_MS / 1000}s`,
  );
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

  const homeMode = normalizeHomeSecurityMode(home?.securityMode);

  if (homeMode === "unprotected") {
    return {
      active: false,
      reason: "home_unprotected",
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

  if (receiverUid !== ownerUid) {
    const sharedMembers =
      sharedByHomeCache.get(homeId) || {};

    if (!sharedMembers?.[receiverUid]) {
      return {
        active: false,
        items: [],
        reason: "home_access_removed",
      };
    }
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
    securityModeRepeatMinutes:
      normalizeSecurityModeRepeatMinutes(
        home?.securityModeRepeatMinutes,
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
    alarmMode:
      customHome?.alarmMode || customHome?.mode || "home",
    customDeviceAlarms,
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

      const previousRepeatMinutes =
        normalizeSecurityModeRepeatMinutes(
          previousHomes[homeId]?.securityModeRepeatMinutes,
        );
      const nextRepeatMinutes =
        normalizeSecurityModeRepeatMinutes(
          nextHomes[homeId]?.securityModeRepeatMinutes,
        );

      if (previousRepeatMinutes !== nextRepeatMinutes) {
        await syncSecurityModeRepeatForHome(
          uid,
          homeId,
          nextHomes[homeId] || null,
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

    const payloadItems = allowedItems.map((item) => ({
      ...item,
      incidentId: String(incidentId || ""),
      eventCategory: getStandardIncidentEventCategory(flowType),
      alarmLevel: getStandardIncidentAlarmLevel(flowType),
    }));

    const message = {
      data: {
        type,
        title,
        body,
        alarmItems: JSON.stringify(payloadItems),
        incidentId: String(incidentId || ""),
        receiverUid: String(uid || ""),
        alarmStage: stage,
        alarmFlowType: flowType,
        incidentSchemaVersion: String(ALARM_INCIDENT_SCHEMA_VERSION),
        eventCategory: getStandardIncidentEventCategory(flowType),
        alarmLevel: getStandardIncidentAlarmLevel(flowType),
        incidentStatus: "active",
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
  flowType = "security",
  status = "resolved",
  hasRemainingActiveIncidents = false,
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
          resolutionAction: String(action || "resolved"),
          resolutionType: getIncidentResolutionType(resolvedBy),
          incidentSchemaVersion: String(ALARM_INCIDENT_SCHEMA_VERSION),
          eventCategory: getStandardIncidentEventCategory(flowType),
          alarmLevel: getStandardIncidentAlarmLevel(flowType),
          incidentStatus: String(status || "resolved"),
          hasRemainingActiveIncidents: String(
            hasRemainingActiveIncidents === true,
          ),
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
        if (incident.physicalSirenEnabled !== false) {
          await requestPhysicalSirenForIncident(
            receiverUid,
            incidentId,
            incident,
            "security_siren_stage",
          );
        }

        if (incident.fullscreenEnabled === false) {
          await incidentRef.update({
            stage: "siren",
            sirenSentAt: now,
            updatedAt: now,
          });
          continue;
        }

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
        if (incident.fullscreenEnabled === false) {
          await incidentRef.update({
            stage: "fullscreen_siren",
            fullscreenSentAt: now,
            updatedAt: now,
          });
          continue;
        }

        if (incident.physicalSirenEnabled !== false) {
          await requestPhysicalSirenForIncident(
            receiverUid,
            incidentId,
            incident,
            "emergency_fullscreen_siren_stage",
          );
        }

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
  {
    homeOverride = null,
    forceDatabase = false,
  } = {},
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

  let home = homeOverride;

  if (!home && !forceDatabase) {
    home = getCachedHomeData(ownerUid, homeId);
  }

  if ((!home || forceDatabase) && ownerUid && homeId) {
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

// Đồng bộ ngay các incident Emergency duy trì (khói/CO/gas/ngập...) khi
// sensor trở lại an toàn. Nhờ vậy còi vật lý và fullscreen Alarm không phải
// chờ tới mốc auto-expire 30 phút mới được dừng.
async function resolveClearedPersistentEmergencyIncidents(
  ownerUid,
  homeId,
  {
    homeOverride = null,
    reason = "persistent_emergency_cleared",
  } = {},
) {
  const receiverUids = getAlarmReceiverUidsForHome(
    ownerUid,
    homeId,
  );

  for (const receiverUid of receiverUids) {
    const incidentsSnap = await db
      .ref(`accounts/${receiverUid}/alarmIncidents`)
      .once("value");
    const incidents = incidentsSnap.val() || {};

    for (const [incidentId, incident] of Object.entries(incidents)) {
      if (
        incident?.status !== "active" ||
        incident?.flowType !== "emergency" ||
        String(incident?.ownerUid || "").trim() !==
          String(ownerUid || "").trim() ||
        String(incident?.homeId || "").trim() !==
          String(homeId || "").trim()
      ) {
        continue;
      }

      const allItems = normalizeAlarmIncidentItems(
        incident.items,
      );
      const nonPersistentItems = allItems.filter((item) => {
        return !isPersistentEmergencyIncidentItem(item);
      });
      const validation = await evaluatePersistentEmergencyIncident(
        incident,
        {
          homeOverride,
          forceDatabase: !homeOverride,
        },
      );

      if (!validation.hasPersistentItems) {
        continue;
      }

      const keptItems = normalizeAlarmIncidentItems([
        ...nonPersistentItems,
        ...validation.activeItems,
        ...validation.unknownItems,
      ]);

      if (keptItems.length === 0) {
        await resolveAlarmIncidentForReceiver({
          receiverUid,
          incidentId,
          ownerUid: String(ownerUid || ""),
          homeId: String(homeId || ""),
          resolvedBy: "safehome_backend",
          action: reason,
        });
        continue;
      }

      if (JSON.stringify(keptItems) !== JSON.stringify(allItems)) {
        const now = Date.now();

        await db
          .ref(`accounts/${receiverUid}/alarmIncidents/${incidentId}`)
          .update({
            items: keptItems,
            reasons: keptItems.map((item) => item.reason),
            updatedAt: now,
          });

        setLocalActiveAlarmIncident(
          receiverUid,
          incidentId,
          {
            ...incident,
            items: keptItems,
            reasons: keptItems.map((item) => item.reason),
            updatedAt: now,
          },
        );
      }
    }
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
    const flowType = String(
      incident.flowType || incident.eventCategory || "emergency",
    ) === "emergency"
      ? "emergency"
      : "security";
    const updates = {
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/schemaVersion`]:
        ALARM_INCIDENT_SCHEMA_VERSION,
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/eventCategory`]:
        getStandardIncidentEventCategory(flowType),
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/alarmLevel`]:
        getStandardIncidentAlarmLevel(flowType),
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/severity`]:
        getLegacyIncidentSeverity(flowType),
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/statusReason`]:
        "auto_expired",
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/resolutionAction`]:
        "auto_expired",
      [`accounts/${receiverUid}/alarmIncidents/${incidentId}/resolutionType`]:
        "automatic",
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

    await reconcilePhysicalSirenForHome(
      String(incident.ownerUid || receiverUid),
      String(incident.homeId || ""),
      {
        useDatabase: true,
        reason: "incident_expired",
      },
    );

    removeLocalActiveAlarmIncident(
      receiverUid,
      targetKey,
    );
    const hasRemainingActiveIncidents =
      hasLocalActiveAlarmIncidentForReceiver(receiverUid);
    clearAlarmIncidentTimers(receiverUid, incidentId);

    await sendAlarmResolvedPush({
      uid: receiverUid,
      incidentId,
      homeId: String(incident.homeId || ""),
      resolvedBy: "safehome_backend",
      action: "auto_expired",
      flowType,
      status: "expired",
      hasRemainingActiveIncidents,
    });

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


function getSecurityModeItems(items) {
  return normalizeAlarmIncidentItems(items).filter((item) => {
    return String(item.alarmSource || "") === "security_mode";
  });
}

function applySecurityModeRepeatToItems(
  items,
  repeatMinutes,
) {
  const normalizedRepeat =
    normalizeSecurityModeRepeatMinutes(repeatMinutes);
  const nextAlarm = getNextAlarmTimeText(normalizedRepeat);

  return normalizeAlarmIncidentItems(items).map((item) => {
    if (String(item.alarmSource || "") !== "security_mode") {
      return item;
    }

    return {
      ...item,
      repeatMinutes: normalizedRepeat,
      nextAlarm,
    };
  });
}

function clearSecurityModeRepeatTimer(
  receiverUid,
  incidentId,
) {
  const key = getAlarmIncidentTimerKey(
    receiverUid,
    incidentId,
  );
  const timers = alarmIncidentTimerMap[key];

  if (!timers?.repeat) {
    return;
  }

  clearTimeout(timers.repeat);
  delete timers.repeat;

  if (Object.keys(timers).length === 0) {
    delete alarmIncidentTimerMap[key];
  } else {
    alarmIncidentTimerMap[key] = timers;
  }
}

function scheduleSecurityModeRepeatTimer(
  receiverUid,
  incidentId,
  incident,
) {
  clearSecurityModeRepeatTimer(receiverUid, incidentId);

  if (
    !incident ||
    incident.status !== "active" ||
    incident.flowType === "emergency" ||
    getSecurityModeItems(incident.items).length === 0
  ) {
    return;
  }

  const repeatMinutes =
    normalizeSecurityModeRepeatMinutes(
      incident.repeatMinutes,
    );
  const nextRepeatAt = Number(incident.nextRepeatAt || 0);

  if (repeatMinutes <= 0 || nextRepeatAt <= 0) {
    return;
  }

  const key = getAlarmIncidentTimerKey(
    receiverUid,
    incidentId,
  );
  const timers = alarmIncidentTimerMap[key] || {};

  timers.repeat = setTimeout(
    () => {
      void handleSecurityModeRepeatDue(
        receiverUid,
        incidentId,
      );
    },
    Math.max(0, nextRepeatAt - Date.now()),
  );

  alarmIncidentTimerMap[key] = timers;
}

async function loadHomeForSecurityModeRepeat(
  ownerUid,
  homeId,
  homeOverride = null,
) {
  if (homeOverride) {
    return homeOverride;
  }

  const cachedHome = getCachedHomeData(ownerUid, homeId);

  if (cachedHome) {
    return cachedHome;
  }

  const homeSnap = await db
    .ref(`accounts/${ownerUid}/homes/${homeId}`)
    .once("value");

  return homeSnap.val();
}

async function ensureSecurityModeRepeatForIncident(
  receiverUid,
  incidentId,
  incident,
  {
    resetFromNow = false,
    homeOverride = null,
    scheduleTimer = true,
  } = {},
) {
  if (
    !incident ||
    incident.status !== "active" ||
    incident.flowType === "emergency"
  ) {
    clearSecurityModeRepeatTimer(receiverUid, incidentId);
    return incident;
  }

  const securityItems = getSecurityModeItems(incident.items);

  if (securityItems.length === 0) {
    clearSecurityModeRepeatTimer(receiverUid, incidentId);
    return incident;
  }

  const ownerUid = String(
    incident.ownerUid || receiverUid,
  ).trim();
  const homeId = String(incident.homeId || "").trim();
  const home = await loadHomeForSecurityModeRepeat(
    ownerUid,
    homeId,
    homeOverride,
  );

  if (!home) {
    console.log(
      "SECURITY MODE REPEAT HOME UNAVAILABLE:",
      receiverUid,
      incidentId,
      ownerUid,
      homeId,
    );
    return incident;
  }

  const modeArmed =
    normalizeHomeSecurityMode(home.securityMode) === "armed";
  const repeatMinutes = modeArmed
    ? normalizeSecurityModeRepeatMinutes(
        home.securityModeRepeatMinutes,
      )
    : 0;
  const now = Date.now();
  const currentRepeatMinutes =
    normalizeSecurityModeRepeatMinutes(
      incident.repeatMinutes,
    );
  const currentNextRepeatAt = Number(
    incident.nextRepeatAt || 0,
  );
  const updatedItems = applySecurityModeRepeatToItems(
    incident.items,
    repeatMinutes,
  );

  let nextRepeatAt = null;

  if (repeatMinutes > 0) {
    const canKeepCurrentDueAt =
      !resetFromNow &&
      currentRepeatMinutes === repeatMinutes &&
      currentNextRepeatAt > 0;

    nextRepeatAt = canKeepCurrentDueAt
      ? currentNextRepeatAt
      : now + repeatMinutes * 60 * 1000;
  }

  const itemsChanged =
    JSON.stringify(updatedItems) !==
    JSON.stringify(normalizeAlarmIncidentItems(incident.items));
  const repeatChanged =
    currentRepeatMinutes !== repeatMinutes;
  const dueAtChanged =
    Number(currentNextRepeatAt || 0) !==
    Number(nextRepeatAt || 0);
  const updateData = {};

  if (itemsChanged) {
    updateData.items = updatedItems;
    updateData.reasons = updatedItems.map(
      (item) => item.reason,
    );
  }

  if (repeatChanged || dueAtChanged || resetFromNow) {
    updateData.repeatMinutes = repeatMinutes;
    updateData.nextRepeatAt = nextRepeatAt;
    updateData.repeatConfiguredAt = now;
  }

  let updatedIncident = {
    ...incident,
    items: updatedItems,
    reasons: updatedItems.map((item) => item.reason),
    repeatMinutes,
    nextRepeatAt,
  };

  if (Object.keys(updateData).length > 0) {
    updateData.updatedAt = now;

    await db
      .ref(
        `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
      )
      .update(updateData);

    updatedIncident = {
      ...updatedIncident,
      ...updateData,
    };

    setLocalActiveAlarmIncident(
      receiverUid,
      incidentId,
      updatedIncident,
    );
  }

  if (repeatMinutes > 0 && scheduleTimer) {
    scheduleSecurityModeRepeatTimer(
      receiverUid,
      incidentId,
      updatedIncident,
    );
  } else if (repeatMinutes === 0) {
    clearSecurityModeRepeatTimer(
      receiverUid,
      incidentId,
    );
  }

  return updatedIncident;
}

async function syncSecurityModeRepeatForHome(
  ownerUid,
  homeId,
  homeOverride = null,
) {
  const receiverUids = getAlarmReceiverUidsForHome(
    ownerUid,
    homeId,
  );

  for (const receiverUid of receiverUids) {
    try {
      const active =
        await loadActiveSecurityIncidentForReceiver(
          receiverUid,
          ownerUid,
          homeId,
        );

      if (!active) {
        continue;
      }

      await ensureSecurityModeRepeatForIncident(
        receiverUid,
        active.incidentId,
        active.incident,
        {
          resetFromNow: true,
          homeOverride,
        },
      );

      console.log(
        "🔁 SECURITY MODE REPEAT UPDATED:",
        receiverUid,
        ownerUid,
        homeId,
        normalizeSecurityModeRepeatMinutes(
          homeOverride?.securityModeRepeatMinutes,
        ),
      );
    } catch (error) {
      console.log(
        "SECURITY MODE REPEAT SYNC ERROR:",
        receiverUid,
        ownerUid,
        homeId,
        error.message,
      );
    }
  }
}

async function handleSecurityModeRepeatDue(
  receiverUid,
  incidentId,
) {
  const lockKey = `${receiverUid}_${incidentId}`;

  if (securityModeRepeatInProgress.has(lockKey)) {
    return;
  }

  securityModeRepeatInProgress.add(lockKey);
  clearSecurityModeRepeatTimer(receiverUid, incidentId);

  try {
    const incidentRef = db.ref(
      `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
    );
    const incidentSnap = await incidentRef.once("value");
    const incident = incidentSnap.val();

    if (!incident || incident.status !== "active") {
      return;
    }

    const validation =
      await validateAndResolveSecurityIncident(
        receiverUid,
        incidentId,
        incident,
        { reasonHint: "security_mode_repeat" },
      );

    if (!validation.active) {
      return;
    }

    let updatedIncident = {
      ...incident,
      items: validation.items,
      reasons: validation.items.map(
        (item) => item.reason,
      ),
    };

    updatedIncident =
      await ensureSecurityModeRepeatForIncident(
        receiverUid,
        incidentId,
        updatedIncident,
        { scheduleTimer: false },
      );

    const repeatMinutes =
      normalizeSecurityModeRepeatMinutes(
        updatedIncident.repeatMinutes,
      );
    const nextRepeatAt = Number(
      updatedIncident.nextRepeatAt || 0,
    );

    if (repeatMinutes <= 0 || nextRepeatAt <= 0) {
      return;
    }

    if (nextRepeatAt > Date.now() + 500) {
      scheduleSecurityModeRepeatTimer(
        receiverUid,
        incidentId,
        updatedIncident,
      );
      return;
    }

    const repeatedItems = getSecurityModeItems(
      updatedIncident.items,
    );

    if (repeatedItems.length === 0) {
      return;
    }

    const sent = await sendAlarmStageSummary(
      receiverUid,
      repeatedItems,
      {
        incidentId,
        stage: "siren",
        flowType: "security",
      },
    );

    const completedAt = Date.now();
    const followingRepeatAt =
      completedAt + repeatMinutes * 60 * 1000;
    const updateData = {
      items: applySecurityModeRepeatToItems(
        updatedIncident.items,
        repeatMinutes,
      ),
      nextRepeatAt: followingRepeatAt,
      lastRepeatAttemptAt: completedAt,
      updatedAt: completedAt,
    };

    updateData.reasons = updateData.items.map(
      (item) => item.reason,
    );

    if (sent) {
      updateData.lastRepeatedAt = completedAt;
      updateData.repeatCount =
        Number(updatedIncident.repeatCount || 0) + 1;
    }

    await incidentRef.update(updateData);

    updatedIncident = {
      ...updatedIncident,
      ...updateData,
    };

    setLocalActiveAlarmIncident(
      receiverUid,
      incidentId,
      updatedIncident,
    );

    scheduleSecurityModeRepeatTimer(
      receiverUid,
      incidentId,
      updatedIncident,
    );

    console.log(
      sent
        ? "🔁 SECURITY MODE ALARM REPEATED:"
        : "⚠️ SECURITY MODE REPEAT PUSH FAILED:",
      receiverUid,
      incidentId,
      `minutes=${repeatMinutes}`,
      `next=${followingRepeatAt}`,
    );
  } catch (error) {
    console.log(
      "SECURITY MODE REPEAT ERROR:",
      receiverUid,
      incidentId,
      error.message,
    );
  } finally {
    securityModeRepeatInProgress.delete(lockKey);
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
      fullscreenSiren: incident.fullscreenEnabled === false
        ? null
        : setTimeout(
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
    siren:
      incident.physicalSirenEnabled === false &&
      incident.fullscreenEnabled === false
        ? null
        : setTimeout(
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

  scheduleSecurityModeRepeatTimer(
    receiverUid,
    incidentId,
    incident,
  );
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
              const source = String(
                item.alarmSource || "scheduled_alarm",
              );

              return (
                source === "scheduled_alarm" &&
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
              ...buildStandardIncidentFields(
                flowType,
                newItems.length > 0
                  ? "sensor_condition_added"
                  : "sensor_condition_repeated",
              ),
              items: mergedItems,
              reasons: mergedItems.map(
                (item) => item.reason,
              ),
              physicalSirenEnabled: mergedItems.some(
                (item) => item.physicalSirenEnabled !== false,
              ),
              fullscreenEnabled: mergedItems.some(
                (item) => item.fullscreenEnabled !== false,
              ),
              triggerDelaySeconds: Math.max(
                0,
                ...mergedItems.map(
                  (item) => Number(item.triggerDelaySeconds || 0),
                ),
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

            const repeatReadyIncident =
              await ensureSecurityModeRepeatForIncident(
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
              `nextRepeatAt=${Number(repeatReadyIncident?.nextRepeatAt || 0)}`,
            );

            return;
          }

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
          const hasPersistentEmergency = [
            ...existingItems,
            ...groupedItems,
          ].some(isPersistentEmergencyIncidentItem);
          const mayMergeTransientRepeat =
            activeAgeMs >= 0 &&
            activeAgeMs <= EMERGENCY_MERGE_WINDOW_MS;

          // Một trạng thái Emergency duy trì chỉ giữ một incident cho đến khi
          // sensor trở lại an toàn. Emergency mới từ sensor khác cũng được gộp
          // vào incident đang chạy. Chỉ một sự kiện transient giống hệt (SOS)
          // sau merge window mới được coi là lần kích hoạt mới.
          if (
            newItems.length > 0 ||
            hasPersistentEmergency ||
            mayMergeTransientRepeat
          ) {
            const mergedItems = normalizeAlarmIncidentItems([
              ...existingItems,
              ...groupedItems,
            ]);
            const updateData = {
              ...buildStandardIncidentFields(
                flowType,
                newItems.length > 0
                  ? "sensor_condition_added"
                  : "sensor_condition_repeated",
              ),
              items: mergedItems,
              reasons: mergedItems.map(
                (item) => item.reason,
              ),
              physicalSirenEnabled: mergedItems.some(
                (item) => item.physicalSirenEnabled !== false,
              ),
              fullscreenEnabled: mergedItems.some(
                (item) => item.fullscreenEnabled !== false,
              ),
              triggerDelaySeconds: 0,
              expireAt:
                now + ALARM_INCIDENT_AUTO_EXPIRE_MS,
              lastNewConditionAt:
                newItems.length > 0
                  ? now
                  : Number(
                      active.incident.lastNewConditionAt ||
                      active.incident.detectedAt ||
                      now,
                    ),
              updatedAt: now,
            };

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

            rescheduleAlarmIncidentExpireTimer(
              uid,
              active.incidentId,
              updateData.expireAt,
            );

            if (newItems.length > 0) {
              const sent = await sendAlarmStageSummary(
                uid,
                newItems,
                {
                  incidentId: active.incidentId,
                  stage: "notification",
                  flowType,
                },
              );

              if (!sent) {
                scheduleInitialAlarmIncidentPushRetry(
                  uid,
                  active.incidentId,
                  "notification",
                  flowType,
                );
              }
            }

            const currentStage = String(
              active.incident.stage || "notification",
            );

            if (
              updateData.physicalSirenEnabled === true &&
              (
                currentStage === "fullscreen_siren" ||
                currentStage === "calling"
              )
            ) {
              await reconcilePhysicalSirenForHome(
                ownerUid,
                homeId,
                {
                  useDatabase: true,
                  reason: "emergency_incident_items_merged",
                },
              );
            }

            console.log(
              "➕ EMERGENCY INCIDENT UPDATED:",
              uid,
              active.incidentId,
              `new=${newItems.length}`,
              `items=${mergedItems.length}`,
              `persistent=${hasPersistentEmergency}`,
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
              ...buildStandardIncidentFields(
                flowType,
                "new_emergency_trigger",
              ),
              status: "superseded",
              supersededAt: now,
              supersededReason: "new_emergency_trigger",
              resolutionAction: "new_emergency_trigger",
              resolutionType: "automatic",
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

          ...buildStandardIncidentFields(
            flowType,
            "sensor_triggered",
          ),

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
          physicalSirenEnabled: groupedItems.some(
            (item) => item.physicalSirenEnabled !== false,
          ),
          fullscreenEnabled: groupedItems.some(
            (item) => item.fullscreenEnabled !== false,
          ),
          triggerDelaySeconds: Math.max(
            0,
            ...groupedItems.map(
              (item) => Number(item.triggerDelaySeconds || 0),
            ),
          ),
          createdAt: now,
          updatedAt: now,
        };

        if (flowType === "emergency") {
          incident.fullscreenDueAt =
            now + EMERGENCY_FULLSCREEN_DELAY_MS;
          incident.callDueAt =
            now + EMERGENCY_CALL_DELAY_MS;
        } else {
          const policyDelayMs =
            Number(incident.triggerDelaySeconds || 0) * 1000;
          incident.alarmDueAt =
            now + Math.max(
              ALARM_INCIDENT_ALARM_DELAY_MS,
              policyDelayMs,
            );
          incident.sirenDueAt =
            now + Math.max(
              ALARM_INCIDENT_SIREN_DELAY_MS,
              policyDelayMs,
            );
          incident.callDueAt =
            now + ALARM_INCIDENT_CALL_DELAY_MS;

          const securityItems = getSecurityModeItems(
            groupedItems,
          );
          const currentHome = getCachedHomeData(
            ownerUid,
            homeId,
          );
          const repeatMinutes = securityItems.length > 0
            ? normalizeSecurityModeRepeatMinutes(
                currentHome?.securityModeRepeatMinutes ??
                securityItems[0].repeatMinutes,
              )
            : 0;

          incident.items = applySecurityModeRepeatToItems(
            groupedItems,
            repeatMinutes,
          );
          incident.reasons = incident.items.map(
            (item) => item.reason,
          );
          incident.repeatMinutes = repeatMinutes;
          incident.nextRepeatAt = repeatMinutes > 0
            ? now + repeatMinutes * 60 * 1000
            : null;
          incident.repeatConfiguredAt = now;
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

          severity: getLegacyIncidentSeverity(flowType),
          eventCategory: getStandardIncidentEventCategory(flowType),
          alarmLevel: getStandardIncidentAlarmLevel(flowType),

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
          incident.items,
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

        const incidentOwnerUid = String(
          incident?.ownerUid || uid,
        ).trim();
        const incidentHomeId = String(
          incident?.homeId || "",
        ).trim();
        const incidentHome = getCachedHomeData(
          incidentOwnerUid,
          incidentHomeId,
        );

        if (incidentHome && isHomeUnprotected(incidentHome)) {
          await resolveAlarmIncidentForReceiver({
            receiverUid: uid,
            incidentId,
            ownerUid: incidentOwnerUid,
            homeId: incidentHomeId,
            resolvedBy: "safehome_backend",
            action: "home_unprotected",
          });
          continue;
        }

        const resumableFlowType = String(
          incident?.flowType || incident?.eventCategory || "security",
        ) === "emergency"
          ? "emergency"
          : "security";
        const standardFields = buildStandardIncidentFields(
          resumableFlowType,
          String(incident?.statusReason || "backend_resumed"),
        );
        let resumableIncident = {
          ...incident,
          flowType: resumableFlowType,
          ...standardFields,
        };

        if (
          Number(incident?.schemaVersion || 0) !==
            ALARM_INCIDENT_SCHEMA_VERSION ||
          String(incident?.eventCategory || "") !==
            standardFields.eventCategory ||
          String(incident?.alarmLevel || "") !==
            standardFields.alarmLevel
        ) {
          await db
            .ref(
              `accounts/${uid}/alarmIncidents/${incidentId}`,
            )
            .update({
              flowType: resumableFlowType,
              ...standardFields,
              updatedAt: Date.now(),
            });
        }

        if (resumableFlowType !== "emergency") {
          const validation =
            await validateAndResolveSecurityIncident(
              uid,
              incidentId,
              resumableIncident,
              { reasonHint: "backend_restart_validation" },
            );

          if (!validation.active) {
            continue;
          }

          resumableIncident = {
            ...resumableIncident,
            items: validation.items,
          };
        }

        if (resumableIncident.flowType !== "emergency") {
          resumableIncident =
            await ensureSecurityModeRepeatForIncident(
              uid,
              incidentId,
              resumableIncident,
              { scheduleTimer: false },
            );
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
  const flowType = String(
    incident.flowType || incident.eventCategory || "security",
  ) === "emergency"
    ? "emergency"
    : "security";
  const resolutionType = getIncidentResolutionType(resolvedBy);
  const updates = {
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/schemaVersion`]:
      ALARM_INCIDENT_SCHEMA_VERSION,
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/eventCategory`]:
      getStandardIncidentEventCategory(flowType),
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/alarmLevel`]:
      getStandardIncidentAlarmLevel(flowType),
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/severity`]:
      getLegacyIncidentSeverity(flowType),
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/statusReason`]:
      String(action || "resolved"),
    [`accounts/${receiverUid}/alarmIncidents/${incidentId}/resolutionType`]:
      resolutionType,
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

  await reconcilePhysicalSirenForHome(
    ownerUid,
    homeId,
    {
      useDatabase: true,
      reason: `incident_resolved:${action}`,
    },
  );

  removeLocalActiveAlarmIncident(
    receiverUid,
    targetKey,
  );

  const hasRemainingActiveIncidents =
    hasLocalActiveAlarmIncidentForReceiver(receiverUid);

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
    flowType,
    status: "resolved",
    hasRemainingActiveIncidents,
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

  if (!firebaseConnected) {
    registerOfflineAlarmDemand(uid, item);
    enqueueOfflineAlarmItem(uid, item);
    return;
  }

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
      registerOfflineAlarmDemand(uid, item);
      enqueueOfflineAlarmItem(uid, item);
    });

    return;
  }

  if (!pendingEventAlarmMap[uid]) {
    pendingEventAlarmMap[uid] = [];
  }

  const itemIdentity = getAlarmIncidentItemIdentity(item);
  const exists = pendingEventAlarmMap[uid].some((oldItem) => {
    return (
      getAlarmIncidentItemIdentity(oldItem) ===
      itemIdentity
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

      if (!firebaseConnected) {
        for (const queuedItem of items) {
          registerOfflineAlarmDemand(uid, queuedItem);
          enqueueOfflineAlarmItem(uid, queuedItem);
        }
        return;
      }

      try {
        await startOrMergeAlarmIncidents(uid, items);
      } catch (error) {
        console.log(
          "SECURITY INCIDENT START ERROR:",
          uid,
          error.message,
        );

        for (const queuedItem of items) {
          registerOfflineAlarmDemand(uid, queuedItem);
          enqueueOfflineAlarmItem(uid, queuedItem);
        }
      }
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

        const customHomeRules =
          user.customRules?.[homeId] || {};
        const reminderMode = String(
          customHomeRules.reminderMode ||
          customHomeRules.mode ||
          "home",
        );

        if (source === "shared" && reminderMode === "custom") {
          const customRaw =
            customHomeRules.notifications?.items || {};

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

Nếu hôm nay bạn có kế hoạch ra/vào nhà trong thời gian Báo động hoạt động,
hãy thiết lập "Tạm tắt Báo động hôm nay" để tránh làm phiền các thành viên khác.`,
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

Nếu hôm nay bạn có kế hoạch ra/vào nhà trong thời gian Báo động hoạt động,
hãy thiết lập "Tạm tắt Báo động hôm nay" để tránh làm phiền các thành viên khác.`,
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


// ================= CENTRAL ALARM ENGINE =================
// Mọi packet cảm biến được chuẩn hóa thành một quyết định duy nhất trước khi
// tạo incident. Firebase chỉ lưu/đồng bộ trạng thái; quyết định Alarm nằm ở Hub.
const SENSOR_EVENT_CATEGORY = Object.freeze({
  EMERGENCY: "emergency",
  SECURITY: "security",
  SYSTEM_WARNING: "system_warning",
  IGNORE: "ignore",
});

const SENSOR_EVENT_SEVERITY = Object.freeze({
  INFO: "info",
  WARNING: "warning",
  ALARM: "alarm",
  EMERGENCY: "emergency",
});

const ALARM_INCIDENT_SCHEMA_VERSION = 2;

function getStandardIncidentEventCategory(flowType) {
  return String(flowType || "").trim() === "emergency"
    ? SENSOR_EVENT_CATEGORY.EMERGENCY
    : SENSOR_EVENT_CATEGORY.SECURITY;
}

function getStandardIncidentAlarmLevel(flowType) {
  return String(flowType || "").trim() === "emergency"
    ? SENSOR_EVENT_SEVERITY.EMERGENCY
    : SENSOR_EVENT_SEVERITY.ALARM;
}

function getLegacyIncidentSeverity(flowType) {
  return String(flowType || "").trim() === "emergency"
    ? "critical"
    : "warning";
}

function getIncidentResolutionType(resolvedBy) {
  return String(resolvedBy || "").trim() === "safehome_backend"
    ? "automatic"
    : "manual";
}

function buildStandardIncidentFields(
  flowType,
  statusReason = "sensor_triggered",
) {
  return {
    schemaVersion: ALARM_INCIDENT_SCHEMA_VERSION,
    eventCategory: getStandardIncidentEventCategory(flowType),
    alarmLevel: getStandardIncidentAlarmLevel(flowType),
    severity: getLegacyIncidentSeverity(flowType),
    statusReason: String(statusReason || "sensor_triggered"),
  };
}

const DEVICE_ALARM_POLICY_DEFAULTS = Object.freeze({
  enabled: true,
  physicalSirenEnabled: true,
  fullscreenEnabled: true,
  triggerDelaySeconds: 0,
});

function normalizeDeviceAlarmPolicy(device, deviceType) {
  const raw =
    device?.alarmPolicy &&
    typeof device.alarmPolicy === "object"
      ? device.alarmPolicy
      : {};

  const isEmergency = isEmergencyDeviceType(deviceType);
  const requestedDelay = Number.parseInt(
    raw.triggerDelaySeconds ?? 0,
    10,
  );

  return {
    // Cảm biến khẩn cấp luôn tham gia Alarm; người dùng chỉ được chọn
    // cách cảnh báo bằng còi vật lý và fullscreen.
    enabled: isEmergency ? true : raw.enabled !== false,
    physicalSirenEnabled:
      raw.physicalSirenEnabled !== false,
    fullscreenEnabled:
      raw.fullscreenEnabled !== false,
    // Emergency không được trì hoãn. Security cho phép tối đa 120 giây.
    triggerDelaySeconds: isEmergency
      ? 0
      : Math.min(
          120,
          Math.max(
            0,
            Number.isFinite(requestedDelay)
              ? requestedDelay
              : 0,
          ),
        ),
  };
}

function getSensorEventCategory(deviceType) {
  if (isEmergencyDeviceType(deviceType)) {
    return SENSOR_EVENT_CATEGORY.EMERGENCY;
  }

  if (isSecurityDeviceType(deviceType)) {
    return SENSOR_EVENT_CATEGORY.SECURITY;
  }

  return SENSOR_EVENT_CATEGORY.SYSTEM_WARNING;
}

function getSensorAlarmEventCode(deviceType, reason) {
  const normalizedType = String(deviceType || "unknown").trim();
  const normalizedReason = String(reason || "")
    .trim()
    .toLowerCase();

  if (
    normalizedReason.includes("bị tháo") ||
    normalizedReason.includes("tamper") ||
    normalizedReason.includes("cạy")
  ) {
    return `${normalizedType}:tamper`;
  }

  if (normalizedType === "sos") return "sos:pressed";
  if (normalizedType === "smoke") return "smoke:active";
  if (normalizedType === "heat") return "heat:active";
  if (normalizedType === "carbon_monoxide") return "co:active";
  if (normalizedType === "gas") return "gas:active";
  if (
    normalizedType === "water_leak" ||
    normalizedType === "flood"
  ) {
    return "water_leak:active";
  }
  if (
    normalizedType === "door" ||
    normalizedType === "window" ||
    normalizedType === "gate"
  ) {
    return `${normalizedType}:open`;
  }
  if (
    normalizedType === "door_lock" ||
    normalizedType === "lock"
  ) {
    return `${normalizedType}:unlocked`;
  }
  if (
    normalizedType === "motion" ||
    normalizedType === "presence"
  ) {
    return `${normalizedType}:motion`;
  }
  if (normalizedType === "vibration") {
    return "vibration:detected";
  }
  if (normalizedType === "glass_break") {
    return "glass_break:detected";
  }

  return `${normalizedType}:${normalizedReason || "trigger"}`;
}

function getSensorAlarmDebounceMs(deviceType, eventCode) {
  const normalizedType = String(deviceType || "").trim();
  const normalizedCode = String(eventCode || "").trim();

  // SOS/action thường gửi lặp nhiều packet cho cùng một lần bấm.
  if (normalizedType === "sos") return 5 * 1000;
  if (normalizedType === "glass_break") return 5 * 1000;
  if (normalizedType === "vibration") return 3 * 1000;
  if (
    normalizedType === "motion" ||
    normalizedType === "presence"
  ) {
    return 2 * 1000;
  }

  // Emergency trạng thái và cửa/khóa chỉ cần chặn packet song song rất ngắn;
  // lần kích hoạt thật sau khi reset vẫn được nhận gần như ngay lập tức.
  if (
    normalizedCode.endsWith(":active") ||
    normalizedCode.endsWith(":open") ||
    normalizedCode.endsWith(":unlocked") ||
    normalizedCode.endsWith(":tamper")
  ) {
    return 1000;
  }

  return 1500;
}

function cleanupSensorAlarmDebounceMap(now = Date.now()) {
  if (
    sensorAlarmEventDebounceMap.size <
    SENSOR_ALARM_DEBOUNCE_MAX_ENTRIES
  ) {
    return;
  }

  for (const [key, lastAcceptedAt] of
    sensorAlarmEventDebounceMap.entries()) {
    if (
      now - Number(lastAcceptedAt || 0) >
      SENSOR_ALARM_DEBOUNCE_MAX_AGE_MS
    ) {
      sensorAlarmEventDebounceMap.delete(key);
    }
  }

  if (
    sensorAlarmEventDebounceMap.size >
    SENSOR_ALARM_DEBOUNCE_MAX_ENTRIES
  ) {
    const sortedEntries = [
      ...sensorAlarmEventDebounceMap.entries(),
    ].sort((a, b) => Number(a[1]) - Number(b[1]));
    const removeCount =
      sensorAlarmEventDebounceMap.size -
      SENSOR_ALARM_DEBOUNCE_MAX_ENTRIES;

    for (const [key] of sortedEntries.slice(0, removeCount)) {
      sensorAlarmEventDebounceMap.delete(key);
    }
  }
}

function shouldAcceptSensorAlarmTrigger({
  receiverUid,
  ownerUid,
  homeId,
  deviceId,
  deviceType,
  reason,
}) {
  const eventCode = getSensorAlarmEventCode(
    deviceType,
    reason,
  );
  const key = [
    String(receiverUid || "").trim(),
    String(ownerUid || "").trim(),
    String(homeId || "").trim(),
    String(deviceId || "").trim(),
    eventCode,
  ].join("|");
  const now = Date.now();
  const debounceMs = getSensorAlarmDebounceMs(
    deviceType,
    eventCode,
  );
  const lastAcceptedAt = Number(
    sensorAlarmEventDebounceMap.get(key) || 0,
  );

  if (
    lastAcceptedAt > 0 &&
    now - lastAcceptedAt < debounceMs
  ) {
    return false;
  }

  sensorAlarmEventDebounceMap.set(key, now);
  cleanupSensorAlarmDebounceMap(now);
  return true;
}

function buildAlarmTriggerFromSensorEvent({
  deviceType,
  deviceName,
  oldDevice,
  updateData,
}) {
  const safeOldDevice = oldDevice || {};
  const safeUpdateData = updateData || {};

  // Emergency: luôn hoạt động, không phụ thuộc Mode Bảo vệ hoặc lịch Alarm.
  if (deviceType === "smoke") {
    if (
      isActiveSignal(safeUpdateData.smoke) &&
      !isActiveSignal(safeOldDevice.smoke)
    ) {
      return {
        category: SENSOR_EVENT_CATEGORY.EMERGENCY,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        reason: `${deviceName}: Phát hiện khói`,
      };
    }
  }

  if (deviceType === "sos") {
    if (safeUpdateData.action !== undefined) {
      return {
        category: SENSOR_EVENT_CATEGORY.EMERGENCY,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        reason: `${deviceName}: SOS được kích hoạt`,
      };
    }
  }

  if (deviceType === "heat") {
    const triggered =
      (
        isActiveSignal(safeUpdateData.heat) &&
        !isActiveSignal(safeOldDevice.heat)
      ) ||
      (
        isActiveSignal(safeUpdateData.heat_alarm) &&
        !isActiveSignal(safeOldDevice.heat_alarm)
      ) ||
      (
        isActiveSignal(
          safeUpdateData.high_temperature_alarm,
        ) &&
        !isActiveSignal(
          safeOldDevice.high_temperature_alarm,
        )
      );

    if (triggered) {
      return {
        category: SENSOR_EVENT_CATEGORY.EMERGENCY,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        reason: `${deviceName}: Phát hiện nhiệt độ nguy hiểm`,
      };
    }
  }

  if (deviceType === "carbon_monoxide") {
    const triggered =
      (
        isActiveSignal(
          safeUpdateData.carbon_monoxide,
        ) &&
        !isActiveSignal(
          safeOldDevice.carbon_monoxide,
        )
      ) ||
      (
        isActiveSignal(safeUpdateData.co_alarm) &&
        !isActiveSignal(safeOldDevice.co_alarm)
      );

    if (triggered) {
      return {
        category: SENSOR_EVENT_CATEGORY.EMERGENCY,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        reason: `${deviceName}: Phát hiện khí CO`,
      };
    }
  }

  if (deviceType === "gas") {
    const triggered =
      (
        isActiveSignal(safeUpdateData.gas) &&
        !isActiveSignal(safeOldDevice.gas)
      ) ||
      (
        isActiveSignal(safeUpdateData.gas_alarm) &&
        !isActiveSignal(safeOldDevice.gas_alarm)
      );

    if (triggered) {
      return {
        category: SENSOR_EVENT_CATEGORY.EMERGENCY,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        reason: `${deviceName}: Phát hiện rò rỉ gas`,
      };
    }
  }

  if (
    deviceType === "water_leak" ||
    deviceType === "flood"
  ) {
    const triggered =
      (
        isActiveSignal(safeUpdateData.water_leak) &&
        !isActiveSignal(safeOldDevice.water_leak)
      ) ||
      (
        isActiveSignal(safeUpdateData.leak) &&
        !isActiveSignal(safeOldDevice.leak)
      ) ||
      (
        isActiveSignal(safeUpdateData.water) &&
        !isActiveSignal(safeOldDevice.water)
      );

    if (triggered) {
      return {
        category: SENSOR_EVENT_CATEGORY.EMERGENCY,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        reason: `${deviceName}: Phát hiện ngập nước`,
      };
    }
  }

  // Tamper của cảm biến emergency vẫn là sự kiện khẩn cấp như logic cũ.
  if (
    isEmergencyDeviceType(deviceType) &&
    safeUpdateData.tamper === true &&
    safeOldDevice.tamper !== true
  ) {
    return {
      category: SENSOR_EVENT_CATEGORY.EMERGENCY,
      severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
      reason: `${deviceName}: Thiết bị bị tháo`,
    };
  }

  // Security: chỉ được phép tạo incident sau khi Alarm Engine xác nhận
  // Mode Bảo vệ hoặc lịch Alarm đang hoạt động.
  if (isSecurityDeviceType(deviceType)) {
    if (
      safeUpdateData.contact === false &&
      safeOldDevice.contact !== false
    ) {
      return {
        category: SENSOR_EVENT_CATEGORY.SECURITY,
        severity: SENSOR_EVENT_SEVERITY.ALARM,
        reason: `${deviceName}: Cửa mở bất thường`,
      };
    }

    if (
      safeUpdateData.tamper === true &&
      safeOldDevice.tamper !== true
    ) {
      return {
        category: SENSOR_EVENT_CATEGORY.SECURITY,
        severity: SENSOR_EVENT_SEVERITY.ALARM,
        reason: `${deviceName}: Thiết bị bị tháo`,
      };
    }

    const motionTriggered =
      (
        isActiveSignal(safeUpdateData.occupancy) &&
        !isActiveSignal(safeOldDevice.occupancy)
      ) ||
      (
        isActiveSignal(safeUpdateData.motion) &&
        !isActiveSignal(safeOldDevice.motion)
      ) ||
      (
        isActiveSignal(safeUpdateData.presence) &&
        !isActiveSignal(safeOldDevice.presence)
      );

    if (
      (
        deviceType === "motion" ||
        deviceType === "presence"
      ) &&
      motionTriggered
    ) {
      return {
        category: SENSOR_EVENT_CATEGORY.SECURITY,
        severity: SENSOR_EVENT_SEVERITY.ALARM,
        reason: `${deviceName}: Phát hiện chuyển động`,
      };
    }

    const vibrationTriggered =
      deviceType === "vibration" &&
      (
        (
          isActiveSignal(safeUpdateData.vibration) &&
          !isActiveSignal(safeOldDevice.vibration)
        ) ||
        (
          isVibrationAction(safeUpdateData.action) &&
          (
            !isVibrationAction(safeOldDevice.action) ||
            Date.now() - Number(
              safeOldDevice.last_vibration_at || 0,
            ) > VIBRATION_ACTIVE_WINDOW_MS
          )
        )
      );

    if (vibrationTriggered) {
      return {
        category: SENSOR_EVENT_CATEGORY.SECURITY,
        severity: SENSOR_EVENT_SEVERITY.ALARM,
        reason: `${deviceName}: Phát hiện rung/chấn động`,
      };
    }

    const glassBreakTriggered =
      deviceType === "glass_break" &&
      (
        (
          (
            isActiveSignal(safeUpdateData.glass_break) ||
            isActiveSignal(safeUpdateData.broken_glass)
          ) &&
          !(
            isActiveSignal(safeOldDevice.glass_break) ||
            isActiveSignal(safeOldDevice.broken_glass)
          )
        ) ||
        (
          isGlassBreakAction(safeUpdateData.action) &&
          !isGlassBreakAction(safeOldDevice.action)
        )
      );

    if (glassBreakTriggered) {
      return {
        category: SENSOR_EVENT_CATEGORY.SECURITY,
        severity: SENSOR_EVENT_SEVERITY.ALARM,
        reason: `${deviceName}: Phát hiện kính vỡ`,
      };
    }

    if (
      (
        deviceType === "door_lock" ||
        deviceType === "lock"
      ) &&
      normalizeLockState({
        ...safeOldDevice,
        ...safeUpdateData,
      }) === "unlocked" &&
      normalizeLockState(safeOldDevice) !== "unlocked"
    ) {
      return {
        category: SENSOR_EVENT_CATEGORY.SECURITY,
        severity: SENSOR_EVENT_SEVERITY.ALARM,
        reason: `${deviceName}: Khóa đã mở`,
      };
    }
  }

  return null;
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

      mode = String(
        customHomeRules.alarmMode ||
        customHomeRules.mode ||
        "home",
      );
      customAlarm =
        customHomeRules.devices?.[deviceId]?.alarm || null;
    } else {
      const customRulesSnap = await db
        .ref(
          `accounts/${receiverUid}/customRules/${homeId}`,
        )
        .once("value");

      const customHomeRules =
        customRulesSnap.val() || {};

      mode = String(
        customHomeRules.alarmMode ||
        customHomeRules.mode ||
        "home",
      );
      customAlarm =
        customHomeRules.devices?.[deviceId]?.alarm || null;
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

function getOfflineAlarmDemandKey(item) {
  return [
    String(item?.ownerUid || "").trim(),
    String(item?.homeId || "").trim(),
    String(item?.deviceId || "").trim(),
    getSensorAlarmEventCode(item?.type, item?.reason),
  ].join("|");
}

function getOfflineAlarmDemandExpiry(item, createdAt) {
  const type = String(item?.type || "").trim();

  if (type === "vibration") {
    return createdAt + VIBRATION_ACTIVE_WINDOW_MS + 5000;
  }

  if (
    type === "sos" ||
    type === "glass_break" ||
    type === "motion" ||
    type === "presence"
  ) {
    return createdAt + OFFLINE_TRANSIENT_ALARM_TTL_MS;
  }

  return createdAt + ALARM_INCIDENT_AUTO_EXPIRE_MS;
}

function isOfflineAlarmDemandStillUnsafe(demand) {
  const item = demand?.item || {};
  const ownerUid = String(item.ownerUid || "").trim();
  const homeId = String(item.homeId || "").trim();
  const home = getCachedHomeData(ownerUid, homeId);

  if (!home) {
    // Không tự ý xóa demand khi snapshot chưa đọc được.
    return Date.now() < Number(demand.expiresAt || 0);
  }

  if (Date.now() >= Number(demand.expiresAt || 0)) {
    return false;
  }

  if (isHomeUnprotected(home)) {
    return false;
  }

  const type = String(item.type || "").trim();

  if (isPersistentEmergencyIncidentItem(item)) {
    return isEmergencyIncidentItemStillUnsafe(home, item);
  }

  if (
    type === "sos" ||
    type === "glass_break"
  ) {
    return true;
  }

  const device = home?.devices?.[item.deviceId] || {};

  if (isSecurityDeviceType(type)) {
    return Boolean(
      getUnsafeSecurityReason(
        item.deviceName || item.deviceId,
        type,
        device,
      ),
    );
  }

  return true;
}

async function activateOfflineAlarmDemand(demandKey) {
  offlineAlarmSirenTimerMap.delete(demandKey);
  const demand = offlineAlarmDemandMap.get(demandKey);

  if (
    !demand ||
    firebaseConnected ||
    demand.item?.physicalSirenEnabled === false ||
    !isOfflineAlarmDemandStillUnsafe(demand)
  ) {
    return;
  }

  demand.sirenStarted = true;
  demand.sirenStartedAt = Date.now();
  offlineAlarmDemandMap.set(demandKey, demand);

  await setPhysicalSirenForHome(
    demand.item.ownerUid,
    demand.item.homeId,
    true,
    {
      force: false,
      reason: "offline_alarm_demand",
    },
  );
}

function registerOfflineAlarmDemand(receiverUid, item) {
  if (!item?.ownerUid || !item?.homeId || !item?.deviceId) {
    return;
  }

  const demandKey = getOfflineAlarmDemandKey(item);
  const now = Date.now();
  const flowType = getAlarmIncidentFlowType([item]);
  const delayMs = flowType === "emergency"
    ? EMERGENCY_FULLSCREEN_DELAY_MS
    : Math.max(
        ALARM_INCIDENT_SIREN_DELAY_MS,
        Number(item.triggerDelaySeconds || 0) * 1000,
      );
  const existing = offlineAlarmDemandMap.get(demandKey);
  const demand = {
    ...(existing || {}),
    receiverUid: String(receiverUid || "").trim(),
    item,
    createdAt: Number(existing?.createdAt || now),
    expiresAt: getOfflineAlarmDemandExpiry(
      item,
      Number(existing?.createdAt || now),
    ),
    sirenStarted: existing?.sirenStarted === true,
  };

  offlineAlarmDemandMap.set(demandKey, demand);

  const oldExpiryTimer = offlineAlarmExpiryTimerMap.get(
    demandKey,
  );

  if (oldExpiryTimer) {
    clearTimeout(oldExpiryTimer);
  }

  const expiryTimer = setTimeout(() => {
    offlineAlarmExpiryTimerMap.delete(demandKey);
    void reconcileOfflineAlarmDemandsForHome(
      item.ownerUid,
      item.homeId,
    ).catch((error) => {
      console.log(
        "OFFLINE ALARM EXPIRY ERROR:",
        item.homeId,
        error.message,
      );
    });
  }, Math.max(100, demand.expiresAt - Date.now() + 100));

  offlineAlarmExpiryTimerMap.set(
    demandKey,
    expiryTimer,
  );

  if (
    demand.sirenStarted ||
    offlineAlarmSirenTimerMap.has(demandKey) ||
    item.physicalSirenEnabled === false
  ) {
    return;
  }

  const timer = setTimeout(() => {
    void activateOfflineAlarmDemand(demandKey).catch((error) => {
      console.log(
        "OFFLINE SIREN START ERROR:",
        item.homeId,
        error.message,
      );
    });
  }, delayMs);

  offlineAlarmSirenTimerMap.set(demandKey, timer);

  console.log(
    "📴 OFFLINE ALARM QUEUED:",
    item.homeId,
    item.deviceId,
    flowType,
    `sirenIn=${Math.round(delayMs / 1000)}s`,
  );
}

async function reconcileOfflineAlarmDemandsForHome(
  ownerUid,
  homeId,
) {
  const cleanOwnerUid = String(ownerUid || "").trim();
  const cleanHomeId = String(homeId || "").trim();
  let activeDemandCount = 0;
  let hadStartedDemand = false;

  for (const [key, demand] of offlineAlarmDemandMap.entries()) {
    const item = demand?.item || {};

    if (
      String(item.ownerUid || "").trim() !== cleanOwnerUid ||
      String(item.homeId || "").trim() !== cleanHomeId
    ) {
      continue;
    }

    if (isOfflineAlarmDemandStillUnsafe(demand)) {
      activeDemandCount++;
      hadStartedDemand =
        hadStartedDemand || demand.sirenStarted === true;
      continue;
    }

    const timer = offlineAlarmSirenTimerMap.get(key);

    if (timer) {
      clearTimeout(timer);
      offlineAlarmSirenTimerMap.delete(key);
    }

    const expiryTimer = offlineAlarmExpiryTimerMap.get(key);

    if (expiryTimer) {
      clearTimeout(expiryTimer);
      offlineAlarmExpiryTimerMap.delete(key);
    }

    hadStartedDemand =
      hadStartedDemand || demand.sirenStarted === true;
    offlineAlarmDemandMap.delete(key);
  }

  if (
    !firebaseConnected &&
    activeDemandCount === 0 &&
    hadStartedDemand
  ) {
    await setPhysicalSirenForHome(
      cleanOwnerUid,
      cleanHomeId,
      false,
      {
        force: true,
        reason: "offline_alarm_cleared",
      },
    );
  }
}

function getCurrentEmergencyReason(
  deviceName,
  deviceType,
  device,
) {
  const name = String(deviceName || "Thiết bị").trim();

  if (
    deviceType === "smoke" &&
    isActiveSignal(device?.smoke)
  ) {
    return `${name}: Phát hiện khói`;
  }

  if (
    deviceType === "heat" &&
    (
      isActiveSignal(device?.heat) ||
      isActiveSignal(device?.heat_alarm) ||
      isActiveSignal(device?.high_temperature_alarm)
    )
  ) {
    return `${name}: Phát hiện nhiệt độ nguy hiểm`;
  }

  if (
    deviceType === "carbon_monoxide" &&
    (
      isActiveSignal(device?.carbon_monoxide) ||
      isActiveSignal(device?.co_alarm)
    )
  ) {
    return `${name}: Phát hiện khí CO`;
  }

  if (
    deviceType === "gas" &&
    (
      isActiveSignal(device?.gas) ||
      isActiveSignal(device?.gas_alarm)
    )
  ) {
    return `${name}: Phát hiện rò rỉ gas`;
  }

  if (
    (
      deviceType === "water_leak" ||
      deviceType === "flood"
    ) &&
    (
      isActiveSignal(device?.water_leak) ||
      isActiveSignal(device?.leak) ||
      isActiveSignal(device?.water)
    )
  ) {
    return `${name}: Phát hiện ngập nước`;
  }

  if (deviceType === "sos") {
    const activeUntil = Number(device?.sos_active_until || 0);
    const lastTriggered = Number(device?.last_triggered || 0);

    if (
      activeUntil > Date.now() ||
      (
        lastTriggered > 0 &&
        Date.now() - lastTriggered <
          OFFLINE_TRANSIENT_ALARM_TTL_MS
      )
    ) {
      return `${name}: SOS được kích hoạt`;
    }
  }

  return "";
}

async function resumeOfflineAlarmDemandsFromSnapshot() {
  if (firebaseConnected) {
    return;
  }

  const accounts = getCachedAccountsObject();
  let resumed = 0;

  for (const [ownerUid, account] of Object.entries(accounts)) {
    const homes = account?.homes || {};

    for (const [homeId, home] of Object.entries(homes)) {
      if (isHomeUnprotected(home)) {
        continue;
      }

      const homeName = String(home?.name || homeId);
      const devices = home?.devices || {};
      const receiverUids = getAlarmReceiverUidsForHome(
        ownerUid,
        homeId,
      );

      for (const [deviceId, device] of Object.entries(devices)) {
        const deviceType = String(
          device?.type || "unknown",
        ).trim();
        const deviceName = String(
          device?.name || deviceId,
        );
        const policy = normalizeDeviceAlarmPolicy(
          device,
          deviceType,
        );

        if (policy.enabled !== true) {
          continue;
        }

        if (isEmergencyDeviceType(deviceType)) {
          const reason = getCurrentEmergencyReason(
            deviceName,
            deviceType,
            device,
          );

          if (!reason) {
            continue;
          }

          for (const receiverUid of receiverUids) {
            const item = {
              ownerUid,
              homeId,
              homeName,
              deviceId,
              deviceName,
              type: deviceType,
              reason,
              severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
              eventCategory: SENSOR_EVENT_CATEGORY.EMERGENCY,
              alarmLevel: SENSOR_EVENT_SEVERITY.EMERGENCY,
              repeatMinutes: 0,
              nextAlarm: "ngay lập tức",
              alarmSource: "emergency_sensor",
              physicalSirenEnabled:
                policy.physicalSirenEnabled,
              fullscreenEnabled:
                policy.fullscreenEnabled,
              triggerDelaySeconds: 0,
            };

            registerOfflineAlarmDemand(receiverUid, item);
            enqueueOfflineAlarmItem(receiverUid, item);
            resumed++;
          }

          continue;
        }

        if (!isSecurityDeviceType(deviceType)) {
          continue;
        }

        const reason = getUnsafeSecurityReason(
          deviceName,
          deviceType,
          device,
        );

        if (!reason) {
          continue;
        }

        for (const receiverUid of receiverUids) {
          const receiverAccount = getCachedAccountData(
            receiverUid,
          );
          const deviceAlarm =
            await resolveDeviceAlarmForReceiver(
              receiverUid,
              homeId,
              deviceId,
              home,
              receiverAccount,
            );
          const securityModeArmed =
            normalizeHomeSecurityMode(
              home.securityMode,
            ) === "armed";
          const scheduleArmed =
            !securityModeArmed &&
            deviceAlarm?.enabled === true &&
            isAlarmAllowedToday(deviceAlarm) &&
            isNowInRange(
              deviceAlarm.start,
              deviceAlarm.end,
            );

          if (!securityModeArmed && !scheduleArmed) {
            continue;
          }

          if (
            scheduleArmed &&
            !await canReceiveAlarm(
              receiverUid,
              homeId,
              ownerUid,
              { respectPause: true },
            )
          ) {
            continue;
          }

          const repeatMinutes = securityModeArmed
            ? normalizeSecurityModeRepeatMinutes(
                home.securityModeRepeatMinutes,
              )
            : normalizeRepeatMinutes(
                deviceAlarm?.repeatMinutes,
              );
          const item = {
            ownerUid,
            homeId,
            homeName,
            deviceId,
            deviceName,
            type: deviceType,
            reason,
            severity: SENSOR_EVENT_SEVERITY.ALARM,
            eventCategory: SENSOR_EVENT_CATEGORY.SECURITY,
            alarmLevel: SENSOR_EVENT_SEVERITY.ALARM,
            repeatMinutes,
            nextAlarm: getNextAlarmTimeText(repeatMinutes),
            alarmSource: securityModeArmed
              ? "security_mode"
              : "scheduled_alarm",
            physicalSirenEnabled:
              policy.physicalSirenEnabled,
            fullscreenEnabled:
              policy.fullscreenEnabled,
            triggerDelaySeconds:
              policy.triggerDelaySeconds,
          };

          registerOfflineAlarmDemand(receiverUid, item);
          enqueueOfflineAlarmItem(receiverUid, item);
          resumed++;
        }
      }
    }
  }

  if (resumed > 0) {
    console.log(
      "📴 OFFLINE ALARMS RESUMED FROM SNAPSHOT:",
      resumed,
    );
  }
}

async function processSensorEventThroughAlarmEngine(
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
  const normalizedDeviceType = String(
    deviceType || "unknown",
  ).trim();
  const oldDevice =
    homeData.devices?.[deviceId] || {};
  const category = getSensorEventCategory(
    normalizedDeviceType,
  );
  const alarmPolicy = normalizeDeviceAlarmPolicy(
    oldDevice,
    normalizedDeviceType,
  );

  if (category === SENSOR_EVENT_CATEGORY.SYSTEM_WARNING) {
    // Pin yếu, offline, Hub offline... chỉ tạo cảnh báo hệ thống ở luồng
    // giám sát sức khỏe; tuyệt đối không đánh thức còi khẩn cấp tại đây.
    return;
  }

  const trigger = buildAlarmTriggerFromSensorEvent({
    deviceType: normalizedDeviceType,
    deviceName,
    oldDevice,
    updateData,
  });

  if (!trigger) {
    return;
  }

  if (
    !shouldAcceptSensorAlarmTrigger({
      receiverUid,
      ownerUid,
      homeId,
      deviceId,
      deviceType: normalizedDeviceType,
      reason: trigger.reason,
    })
  ) {
    console.log(
      "🧯 SENSOR ALARM PACKET DEBOUNCED:",
      receiverUid,
      homeId,
      deviceId,
      normalizedDeviceType,
    );
    return;
  }

  const homeMode = normalizeHomeSecurityMode(homeData.securityMode);

  if (homeMode === "unprotected") {
    await sendUnprotectedSensorNotification(receiverUid, {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      deviceType: normalizedDeviceType,
      reason: trigger.reason,
      eventCategory: trigger.category,
    });
    return;
  }

  if (alarmPolicy.enabled !== true) {
    return;
  }

  if (trigger.category === SENSOR_EVENT_CATEGORY.EMERGENCY) {
    const alarmItem = {
      ownerUid,
      homeId,
      homeName,
      deviceId,
      deviceName,
      type: normalizedDeviceType,
      reason: trigger.reason,
      severity: trigger.severity,
      eventCategory: trigger.category,
      repeatMinutes: 0,
      nextAlarm: "ngay lập tức",
      alarmSource: "emergency_sensor",
      alarmLevel: trigger.severity,
      physicalSirenEnabled:
        alarmPolicy.physicalSirenEnabled,
      fullscreenEnabled:
        alarmPolicy.fullscreenEnabled,
      triggerDelaySeconds: 0,
    };

    if (!firebaseConnected) {
      registerOfflineAlarmDemand(receiverUid, alarmItem);
    }

    queueEventAlarm(receiverUid, alarmItem);
    return;
  }

  const deviceAlarm =
    await resolveDeviceAlarmForReceiver(
      receiverUid,
      homeId,
      deviceId,
      homeData,
      getCachedAccountData(receiverUid),
    );

  const securityModeArmed = homeMode === "armed";

  const scheduleArmed =
    !securityModeArmed &&
    deviceAlarm?.enabled === true &&
    isAlarmAllowedToday(deviceAlarm) &&
    isNowInRange(
      deviceAlarm.start,
      deviceAlarm.end,
    );

  // Security chỉ hoạt động khi Mode Bảo vệ hoặc lịch của sensor đang bật.
  if (!securityModeArmed && !scheduleArmed) {
    return;
  }

  // Pause Today chỉ áp dụng cho Alarm theo lịch. Mode Bảo vệ vẫn giữ nguyên
  // hành vi hiện tại và được kiểm soát bằng chuyển Mode Bình thường.
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

  if (!securityModeArmed) {
    const alarmKey = getScheduleAlarmKey(
      receiverUid,
      ownerUid,
      homeId,
      deviceId,
      deviceAlarm,
    );

    lastScheduleAlarmMap[alarmKey] = Date.now();
  }

  const alarmItem = {
    ownerUid,
    homeId,
    homeName,
    deviceId,
    deviceName,
    type: normalizedDeviceType,
    reason: trigger.reason,
    severity: trigger.severity,
    eventCategory: trigger.category,
    repeatMinutes,
    nextAlarm: getNextAlarmTimeText(repeatMinutes),
    alarmSource: securityModeArmed
      ? "security_mode"
      : "scheduled_alarm",
    alarmLevel: trigger.severity,
    physicalSirenEnabled:
      alarmPolicy.physicalSirenEnabled,
    fullscreenEnabled:
      alarmPolicy.fullscreenEnabled,
    triggerDelaySeconds:
      alarmPolicy.triggerDelaySeconds,
  };

  if (!firebaseConnected) {
    registerOfflineAlarmDemand(receiverUid, alarmItem);
  }

  queueEventAlarm(receiverUid, alarmItem);
}

// Giữ tên hàm cũ để các luồng CO fast-path và MQTT hiện tại không cần đổi
// đồng loạt. Mọi lời gọi đều được chuyển qua Alarm Engine trung tâm.
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
  return processSensorEventThroughAlarmEngine(
    receiverUid,
    ownerUid,
    homeId,
    homeName,
    deviceId,
    deviceName,
    deviceType,
    homeData,
    updateData,
  );
}

async function checkScheduledAlarms() {
  console.log("🚨 CHECK PER-DEVICE ALARM SCHEDULE");

  try {
    const accounts = getCachedAccountsObject();
    const now = Date.now();
    const alarmSummaryByUser = {};

    for (const [ownerUid, ownerAccount] of Object.entries(accounts)) {
      const homes = ownerAccount?.homes || {};

      for (const [homeId, home] of Object.entries(homes)) {
        const homeMode = normalizeHomeSecurityMode(home?.securityMode);
        const securityModeArmed = homeMode === "armed";

        // Bảo vệ dùng incident riêng; Không bảo vệ tắt toàn bộ Alarm.
        if (securityModeArmed || homeMode === "unprotected") {
          continue;
        }

        const receiverUids = getAlarmReceiverUidsForHome(
          ownerUid,
          homeId,
        );
        const devices = home?.devices || {};

        for (const receiverUid of receiverUids) {
          const receiverAccount = accounts[receiverUid] || {};

          for (const [deviceId, device] of Object.entries(devices)) {
            const deviceType = String(
              device?.type || "door",
            ).trim();

            if (!isSecurityDeviceType(deviceType)) {
              continue;
            }

            delete lastScheduleAlarmMap[
              getSecurityModeAlarmKey(
                receiverUid,
                ownerUid,
                homeId,
                deviceId,
              )
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

            const alarmKey = getScheduleAlarmKey(
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

            const canReceive = await canReceiveAlarm(
              receiverUid,
              homeId,
              ownerUid,
              { respectPause: true },
            );

            if (!canReceive) {
              continue;
            }

            const deviceName = String(
              device?.name || deviceId,
            );
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
              deviceId,
              deviceName,
              type: deviceType,
              reason,
              repeatMinutes,
              nextAlarm:
                getNextAlarmTimeText(repeatMinutes),
              alarmSource: "scheduled_alarm",
            });
          }
        }
      }
    }

    for (const [receiverUid, items] of Object.entries(
      alarmSummaryByUser,
    )) {
      try {
        await startOrMergeAlarmIncidents(
          receiverUid,
          items,
        );
      } catch (error) {
        console.log(
          "SCHEDULED ALARM RECEIVER ERROR:",
          receiverUid,
          error.message,
        );
      }
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
  const mode = String(value || "").trim().toLowerCase();

  if (mode === "armed" || mode === "unprotected") {
    return mode;
  }

  return "normal";
}

function isHomeUnprotected(home) {
  return normalizeHomeSecurityMode(home?.securityMode) === "unprotected";
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
    const homeSnap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}`)
      .once("value");
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

    const receiverUids = getAlarmReceiverUidsForHome(
      ownerUid,
      homeId,
    );
    let successfulReceivers = 0;

    for (const receiverUid of receiverUids) {
      try {
        await startOrMergeAlarmIncidents(
          receiverUid,
          alarmItems,
        );
        successfulReceivers++;
      } catch (error) {
        console.log(
          "SECURITY MODE RECEIVER ALARM ERROR:",
          receiverUid,
          ownerUid,
          homeId,
          error.message,
        );
      }
    }

    console.log(
      "🚨 SECURITY MODE ARMED WITH EXISTING UNSAFE STATE:",
      ownerUid,
      homeId,
      `items=${alarmItems.length}`,
      `receivers=${successfulReceivers}/${receiverUids.length}`,
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

async function resolveAllAlarmIncidentsForHome(
  ownerUid,
  homeId,
  action = "home_unprotected",
) {
  const receiverUids = getAlarmReceiverUidsForHome(ownerUid, homeId);
  let resolved = 0;

  for (const receiverUid of receiverUids) {
    try {
      const incidentsSnap = await db
        .ref(`accounts/${receiverUid}/alarmIncidents`)
        .once("value");
      const incidents = incidentsSnap.val() || {};

      for (const [incidentId, incident] of Object.entries(incidents)) {
        if (
          incident?.status !== "active" ||
          String(incident.ownerUid || "") !== String(ownerUid) ||
          String(incident.homeId || "") !== String(homeId)
        ) {
          continue;
        }

        const didResolve = await resolveAlarmIncidentForReceiver({
          receiverUid,
          incidentId,
          ownerUid,
          homeId,
          resolvedBy: "safehome_backend",
          action,
        });

        if (didResolve) {
          resolved++;
        }
      }
    } catch (error) {
      console.log(
        "UNPROTECTED INCIDENT RESOLVE ERROR:",
        receiverUid,
        ownerUid,
        homeId,
        error.message,
      );
    }
  }

  for (const [key, demand] of Array.from(offlineAlarmDemandMap.entries())) {
    const item = demand?.item || {};

    if (
      String(item.ownerUid || "") === String(ownerUid) &&
      String(item.homeId || "") === String(homeId)
    ) {
      clearOfflineAlarmDemand(key);
    }
  }

  await setPhysicalSirenForHome(ownerUid, homeId, false, {
    force: true,
    reason: action,
  });

  console.log(
    "🛡️ HOME UNPROTECTED, ALARMS RESOLVED:",
    ownerUid,
    homeId,
    `incidents=${resolved}`,
  );
}

async function triggerEmergencyForCurrentUnsafeState(
  ownerUid,
  homeId,
) {
  try {
    const homeSnap = await db
      .ref(`accounts/${ownerUid}/homes/${homeId}`)
      .once("value");
    const home = homeSnap.val();

    if (!home || isHomeUnprotected(home)) {
      return;
    }

    const homeName = String(home.name || homeId).trim() || homeId;
    const items = [];

    for (const [deviceId, rawDevice] of Object.entries(home.devices || {})) {
      const device = rawDevice || {};
      const deviceType = String(device.type || "unknown").trim();

      if (!isEmergencyDeviceType(deviceType)) {
        continue;
      }

      const deviceName = String(device.name || deviceId).trim() || deviceId;
      const reason = getCurrentEmergencyReason(
        deviceName,
        deviceType,
        device,
      );

      if (!reason) {
        continue;
      }

      const policy = normalizeDeviceAlarmPolicy(device, deviceType);

      items.push({
        ownerUid,
        homeId,
        homeName,
        deviceId,
        deviceName,
        type: deviceType,
        reason,
        severity: SENSOR_EVENT_SEVERITY.EMERGENCY,
        eventCategory: SENSOR_EVENT_CATEGORY.EMERGENCY,
        alarmLevel: SENSOR_EVENT_SEVERITY.EMERGENCY,
        repeatMinutes: 0,
        nextAlarm: "ngay lập tức",
        alarmSource: "emergency_sensor",
        physicalSirenEnabled: policy.physicalSirenEnabled,
        fullscreenEnabled: policy.fullscreenEnabled,
        triggerDelaySeconds: 0,
      });
    }

    if (items.length === 0) {
      return;
    }

    for (const receiverUid of getAlarmReceiverUidsForHome(ownerUid, homeId)) {
      await startOrMergeAlarmIncidents(receiverUid, items);
    }
  } catch (error) {
    console.log(
      "MODE CHANGE EMERGENCY RECHECK ERROR:",
      ownerUid,
      homeId,
      error.message,
    );
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
          void triggerEmergencyForCurrentUnsafeState(
            ownerUid,
            homeId,
          );
        }, 1000);
      } else if (nextMode === "unprotected") {
        setTimeout(() => {
          void resolveAllAlarmIncidentsForHome(
            ownerUid,
            homeId,
            "home_unprotected",
          );
        }, 200);
      }

      return;
    }

    const previousMode = securityModeLastValueMap.get(key);
    securityModeLastValueMap.set(key, nextMode);

    if (nextMode === "unprotected") {
      void resolveAllAlarmIncidentsForHome(
        ownerUid,
        homeId,
        "home_unprotected",
      );
      return;
    }

    if (nextMode === "armed" && previousMode !== "armed") {
      void triggerAlarmForUnsafeStateOnArmed(ownerUid, homeId);
      void triggerEmergencyForCurrentUnsafeState(ownerUid, homeId);
      return;
    }

    if (nextMode === "normal") {
      const cachedHome = getCachedHomeData(ownerUid, homeId) || {};

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

      if (previousMode === "unprotected") {
        void triggerEmergencyForCurrentUnsafeState(ownerUid, homeId);
      }
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

      const currentMode = normalizeHomeSecurityMode(
        home.securityMode,
      );

      securityModeLastValueMap.set(
        key,
        currentMode,
      );

      if (currentMode === "armed") {
        setTimeout(() => {
          void triggerAlarmForUnsafeStateOnArmed(ownerUid, homeId);
          void triggerEmergencyForCurrentUnsafeState(ownerUid, homeId);
        }, 1000);
      } else if (currentMode === "unprotected") {
        setTimeout(() => {
          void resolveAllAlarmIncidentsForHome(
            ownerUid,
            homeId,
            "home_unprotected",
          );
        }, 200);
      }
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

        // Không bảo vệ là khóa ưu tiên tuyệt đối. Auto Away không được
        // tự bật Bảo vệ hoặc thay đổi mode này.
        if (securityMode === "unprotected") {
          const nextRuntime = buildRuntime({
            status: "unprotected_override",
            ...runtimeCounts,
            allOutsideSince: 0,
            cycleArmed: false,
            manualNormalSnoozeUntil: 0,
            insideOverrideUid: "",
            insideOverrideAt: 0,
            now,
          });

          if (runtimeSignature(runtime) !== runtimeSignature(nextRuntime)) {
            updates[runtimePath] = nextRuntime;
          }

          continue;
        }

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
function awaitWithTimeout(promise, timeoutMs, label) {
  let timeout;

  const timeoutPromise = new Promise((_, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(`${label}_timeout`));
    }, timeoutMs);
  });

  return Promise.race([promise, timeoutPromise])
    .finally(() => {
      clearTimeout(timeout);
    });
}

async function runCloudInitStep(
  label,
  task,
  timeoutMs = 2 * 1000,
) {
  try {
    await awaitWithTimeout(
      Promise.resolve().then(task),
      timeoutMs,
      label,
    );
  } catch (error) {
    console.log(`${label} DEFERRED:`, error.message);
  }
}

async function init() {
  loadLocalRuntimeState();
  startFirebaseConnectionMonitor();
  startOfflineQueueFlushTimer();

  await runCloudInitStep(
    "BACKEND DATA CACHE",
    startBackendDataCache,
    5 * 1000,
  );

  if (!firebaseConnected) {
    await resumeOfflineAlarmDemandsFromSnapshot();
  }

  await runCloudInitStep(
    "OLD PAIR REQUEST CLEANUP",
    async () => {
      await db.ref("pair_requests").remove();
      console.log("🧹 OLD PAIR REQUESTS CLEARED");
    },
  );

  await runCloudInitStep(
    "LEGACY SECURITY SCHEDULE CLEANUP",
    cleanupLegacySecurityScheduleState,
  );

  await runCloudInitStep(
    "CHAT UNREAD MIGRATION",
    ensureChatUnreadCounterMigration,
  );

  await runCloudInitStep(
    "ACTIVE ALARM RESUME",
    resumeActiveAlarmIncidents,
  );

  await runCloudInitStep(
    "PHYSICAL SIREN STARTUP RECONCILE",
    async () => {
      await reconcileAllPhysicalSirens({
        force: true,
        reason: "backend_startup",
      });
    },
  );

  startPhysicalSirenMonitor();
  startAlarmIncidentWatchdog();

  await runCloudInitStep(
    "SECURITY MODE MONITOR",
    startSecurityModeTransitionMonitor,
  );

  try {
    startAutoAwayMonitor({ db });
  } catch (error) {
    console.log(
      "AUTO AWAY MONITOR START ERROR:",
      error.message,
    );
  }

  startHubHeartbeat();
  startSystemHealthMonitor();

  setInterval(cleanupExpiredAlarmPause, 60000);
  setInterval(checkScheduledNotifications, 60000);
  setInterval(() => {
    if (firebaseConnected) {
      void checkScheduledAlarms();
      return;
    }

    void resumeOfflineAlarmDemandsFromSnapshot().catch((error) => {
      console.log(
        "OFFLINE SCHEDULE CHECK ERROR:",
        error.message,
      );
    });
  }, 60000);

  console.log(
    "🛡️ SAFEHOME BACKEND READY:",
    firebaseConnected ? "cloud" : "offline_local",
  );
}

init().catch((error) => {
  console.log("BACKEND INIT ERROR:", error.message);
});

function persistRuntimeBeforeExit(signal) {
  try {
    if (localRuntimeSnapshotSaveTimer) {
      clearTimeout(localRuntimeSnapshotSaveTimer);
      localRuntimeSnapshotSaveTimer = null;
    }

    if (offlineQueueSaveTimer) {
      clearTimeout(offlineQueueSaveTimer);
      offlineQueueSaveTimer = null;
    }

    persistLocalRuntimeSnapshotNow();
    persistOfflineQueueNow();
  } finally {
    console.log("💾 LOCAL RUNTIME SAVED:", signal);
  }
}

process.once("SIGTERM", () => {
  persistRuntimeBeforeExit("SIGTERM");
  process.exit(0);
});

process.once("SIGINT", () => {
  persistRuntimeBeforeExit("SIGINT");
  process.exit(0);
});
const deviceDeleteInProgress = new Set();

db.ref("device_delete_requests").on("child_added", async (snap) => {
  const requestId = String(snap.key || "").trim();
  const req = snap.val() || {};

  if (!requestId || req.status !== "pending") {
    return;
  }

  const ownerUid = String(req.ownerUid || "").trim();
  const homeId = String(req.homeId || "").trim();
  const deviceId = String(req.deviceId || "").trim();
  const requestedBy = String(req.requestedBy || "").trim();
  const operationKey = `${ownerUid}|${homeId}|${deviceId}`;

  if (deviceDeleteInProgress.has(operationKey)) {
    return;
  }

  deviceDeleteInProgress.add(operationKey);

  try {
    if (!ownerUid || !homeId || !deviceId || !requestedBy) {
      console.log(
        "❌ DEVICE DELETE REQUEST INVALID:",
        requestId,
      );

      await snap.ref.remove();
      return;
    }

    const deviceRef = db.ref(
      `accounts/${ownerUid}/homes/${homeId}/devices/${deviceId}`,
    );

    const deviceSnap = await deviceRef.once("value");

    if (!deviceSnap.exists()) {
      await db.ref().update({
        [`system/devices_by_ieee/${deviceId}`]: null,
        [`device_delete_requests/${requestId}`]: null,
      });

      delete deviceMap[deviceId];

      console.log(
        "🧹 DEVICE ALREADY REMOVED:",
        deviceId,
      );
      return;
    }

    console.log(
      "🗑️ DELETE DEVICE:",
      deviceId,
      `owner=${ownerUid}`,
      `requestedBy=${requestedBy}`,
    );

    await new Promise((resolve, reject) => {
      client.publish(
        "zigbee2mqtt/bridge/request/device/remove",
        JSON.stringify({
          id: deviceId,
          force: true,
        }),
        (error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        },
      );
    });

    // Cho Zigbee2MQTT đủ thời gian xoá thiết bị trước khi dọn Firebase.
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const updates = {
      [`accounts/${ownerUid}/homes/${homeId}/devices/${deviceId}`]: null,
      [`system/devices_by_ieee/${deviceId}`]: null,
      [`device_delete_requests/${requestId}`]: null,
    };

    // Dọn cấu hình Alarm riêng còn sót của Owner và các thành viên.
    const affectedUids = new Set([ownerUid]);
    const sharedMembers = sharedByHomeCache.get(homeId) || {};

    for (const sharedUid of Object.keys(sharedMembers)) {
      const cleanUid = String(sharedUid || "").trim();

      if (cleanUid) {
        affectedUids.add(cleanUid);
      }
    }

    for (const affectedUid of affectedUids) {
      updates[
        `accounts/${affectedUid}/customRules/${homeId}/devices/${deviceId}`
      ] = null;
    }

    await db.ref().update(updates);

    delete deviceMap[deviceId];

    console.log(
      "✅ DEVICE REMOVED:",
      deviceId,
      `request=${requestId}`,
    );
  } catch (error) {
    console.log(
      "DELETE DEVICE ERROR:",
      requestId,
      error.message,
    );

    // Xoá request lỗi để người dùng có thể gửi lại ngay.
    try {
      await snap.ref.remove();
    } catch (_) {}
  } finally {
    deviceDeleteInProgress.delete(operationKey);
  }
});

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
        `Báo động đã được ${actorName} tạm tắt từ ${pause.start} tới ${pause.end}.

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
        `${actorName} đã tạm dừng Báo động từ ${pause.start} đến ${pause.end}.` +
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
          title: "Báo động đã được tạm dừng",
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

// ================= HOME SIREN ACTION REQUEST =================
// Nút trong DeviceList chỉ tắt còi vật lý của Home. Incident, fullscreen và
// notification vẫn tiếp tục cho đến khi sự cố được xử lý hoặc tự kết thúc.
function scheduleHomeSirenActionRequestCleanup(requestRef) {
  setTimeout(() => {
    void requestRef.remove().catch(() => { });
  }, HOME_SIREN_ACTION_RESULT_TTL_MS);
}

async function finishHomeSirenActionRequest(
  requestRef,
  status,
  details = {},
) {
  await requestRef.update({
    status,
    ...details,
    completedAt: Date.now(),
  });

  scheduleHomeSirenActionRequestCleanup(requestRef);
}

db.ref("home_siren_action_requests").on(
  "child_added",
  async (snap) => {
    const req = snap.val();
    const requestId = snap.key;
    let ownsRequest = false;

    async function reject(reason) {
      console.log(
        "❌ HOME SIREN ACTION REJECTED:",
        requestId,
        reason,
      );

      try {
        await finishHomeSirenActionRequest(
          snap.ref,
          "failed",
          {
            reason,
            processingHubId: DEVICE_ID,
          },
        );
      } catch (_) { }
    }

    try {
      if (!req || !requestId) {
        return;
      }

      if (
        req.status === "succeeded" ||
        req.status === "failed" ||
        req.status === "rejected"
      ) {
        scheduleHomeSirenActionRequestCleanup(snap.ref);
        return;
      }

      if (req.status !== "pending") {
        return;
      }

      if (homeSirenActionInProgress.has(requestId)) {
        return;
      }

      homeSirenActionInProgress.add(requestId);

      const homeId = String(req.homeId || "").trim();
      const requestedHubId = String(req.hubId || "").trim();
      const requestedBy = String(
        req.requestedBy || "",
      ).trim();
      const action = String(req.action || "").trim();
      const createdAt = Number(req.createdAt);
      const now = Date.now();

      if (
        !homeId ||
        !requestedHubId ||
        !requestedBy ||
        action !== "mute" ||
        !Number.isFinite(createdAt) ||
        createdAt > now + 1000 ||
        createdAt < now - 5 * 60 * 1000
      ) {
        ownsRequest = true;
        await reject("invalid_request");
        return;
      }

      let requesterAccount =
        getCachedAccountData(requestedBy);

      if (!requesterAccount) {
        const accountSnap = await db
          .ref(`accounts/${requestedBy}`)
          .once("value");
        requesterAccount = accountSnap.val() || null;
      }

      let ownerUid = "";

      if (requesterAccount?.homes?.[homeId]) {
        ownerUid = requestedBy;
      } else {
        ownerUid = String(
          requesterAccount?.sharedHomes?.[homeId]?.ownerUid || "",
        ).trim();
      }

      if (!ownerUid) {
        ownsRequest = true;
        await reject("home_access_denied");
        return;
      }

      let home = getCachedHomeData(ownerUid, homeId);

      if (!home) {
        const homeSnap = await db
          .ref(`accounts/${ownerUid}/homes/${homeId}`)
          .once("value");
        home = homeSnap.val() || null;
      }

      if (!home) {
        ownsRequest = true;
        await reject("home_not_found");
        return;
      }

      // hubId lưu trong Home là nguồn hiện tại. hubId từ app chỉ là gợi ý để
      // request vẫn đi được khi cache UI vừa cũ hoặc heartbeat vừa đổi Hub.
      const homeHubId = String(home.hubId || "").trim();
      const targetHubId = homeHubId || requestedHubId;

      if (targetHubId !== DEVICE_ID) {
        return;
      }

      ownsRequest = true;

      await snap.ref.update({
        status: "processing",
        processingHubId: DEVICE_ID,
        startedAt: Date.now(),
      });

      const result = await mutePhysicalSirenForHome(
        ownerUid,
        homeId,
        requestedBy,
        { reason: "device_list_mute_button" },
      );

      const succeeded = result.status === "stopped";

      console.log(
        "🔕 HOME SIREN MUTED FROM DEVICE LIST:",
        requestId,
        requestedBy,
        ownerUid,
        homeId,
        result.status,
        `confirmed=${result.confirmedCount || 0}/${result.deviceCount || 0}`,
      );

      await finishHomeSirenActionRequest(
        snap.ref,
        succeeded ? "succeeded" : "failed",
        {
          resultStatus: result.status,
          ownerUid,
          homeId,
          hubId: DEVICE_ID,
          deviceCount: Number(result.deviceCount || 0),
          successCount: Number(result.successCount || 0),
          confirmedCount: Number(result.confirmedCount || 0),
          reason: succeeded ? "" : result.status,
        },
      );
    } catch (error) {
      console.log(
        "HOME SIREN ACTION ERROR:",
        requestId,
        error.message,
      );

      if (ownsRequest) {
        try {
          await finishHomeSirenActionRequest(
            snap.ref,
            "failed",
            {
              reason: "backend_error",
              error: String(error.message || "").slice(0, 300),
              processingHubId: DEVICE_ID,
            },
          );
        } catch (_) { }
      }
    } finally {
      if (requestId) {
        homeSirenActionInProgress.delete(requestId);
      }
    }
  },
);

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
        "mute_siren",
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

      // Tắt riêng còi vật lý của Home nhưng giữ nguyên incident, fullscreen
      // và notification. Mute được gắn với snapshot các incident active hiện
      // tại; sự cố mới sau đó vẫn có thể bật còi trở lại.
      if (action === "mute_siren") {
        if (incident.status === "active") {
          await mutePhysicalSirenForHome(
            ownerUid,
            homeId,
            requestedBy,
            { reason: "manual_mute_button" },
          );

          await db
            .ref(
              `accounts/${receiverUid}/alarmIncidents/${incidentId}`,
            )
            .update({
              homeSirenStatus: "manual_muted",
              homeSirenMutedAt: now,
              homeSirenMutedBy: requestedBy,
              updatedAt: now,
            });
        }

        await snap.ref.remove();

        console.log(
          "🔕 ALARM HOME SIREN MUTED:",
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
          flowType: String(
            incident.flowType || incident.eventCategory || "security",
          ),
          status: String(incident.status || "resolved"),
          hasRemainingActiveIncidents:
            hasLocalActiveAlarmIncidentForReceiver(receiverUid),
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

      // Nút "Tắt cảnh báo" cũng phải tắt còi cho toàn Home. Nếu chỉ
      // resolve incident của tài khoản hiện tại, các bản sao incident của
      // thành viên khác sẽ khiến vòng reconcile bật còi trở lại sau 15 giây.
      if (action === "stop") {
        await mutePhysicalSirenForHome(
          ownerUid,
          homeId,
          requestedBy,
          { reason: "stop_alarm_button" },
        );
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

// ================= MQTT CONNECT =================
client.on("connect", () => {
  mqttConnected = true;

  console.log("MQTT CONNECTED");
  client.subscribe("zigbee2mqtt/#");

  // Cập nhật ngay để app biết MQTT đã hoạt động.
  void writeHubHeartbeat();

  // Nếu MQTT vừa mất kết nối trong lúc có Alarm, gửi lại trạng thái còi.
  setTimeout(() => {
    void reconcileAllPhysicalSirens({
      force: true,
      reason: "mqtt_connected",
    });
  }, 1000);
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

// ================= CO SENSOR FAST PATH =================
function isCarbonMonoxidePayload(data) {
  return Boolean(
    data &&
    typeof data === "object" &&
    (
      data.carbon_monoxide !== undefined ||
      data.co_alarm !== undefined ||
      data.co !== undefined
    )
  );
}

function telemetryValueChanged(previousValue, nextValue) {
  if (nextValue === undefined) {
    return false;
  }

  const previousNumber = Number(previousValue);
  const nextNumber = Number(nextValue);

  if (
    Number.isFinite(previousNumber) &&
    Number.isFinite(nextNumber)
  ) {
    return previousNumber !== nextNumber;
  }

  return previousValue !== nextValue;
}

async function processCarbonMonoxidePacket(
  deviceId,
  uid,
  homeId,
  data,
) {
  const deviceRef = db.ref(
    `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
  );

  let runtime = coSensorRuntimeMap.get(deviceId);

  // Ưu tiên snapshot cục bộ để CO vẫn xử lý được ngay khi Firebase mất mạng.
  if (!runtime) {
    let persistedDevice =
      getCachedHomeData(uid, homeId)?.devices?.[deviceId] || null;

    if (!persistedDevice && firebaseConnected) {
      try {
        const deviceSnap = await deviceRef.once("value");
        persistedDevice = deviceSnap.val();
      } catch (error) {
        console.log(
          "CO FIREBASE READ FALLBACK:",
          deviceId,
          error.message,
        );
      }
    }

    if (!persistedDevice) {
      console.log(
        "⚠️ CO SKIPPED, NO LOCAL SNAPSHOT:",
        deviceId,
      );
      return;
    }

    runtime = {
      device: { ...persistedDevice },
      persistedCarbonMonoxide:
        persistedDevice.carbon_monoxide,
      persistedCoAlarm: persistedDevice.co_alarm,
      persistedCo: persistedDevice.co,
      persistedLastSeen: persistedDevice.last_seen,
      persistedLinkquality: persistedDevice.linkquality,
      lastCoPersistAt: 0,
      lastTelemetryPersistAt: 0,
    };

    coSensorRuntimeMap.set(deviceId, runtime);
  }

  const now = Date.now();
  const oldDevice = { ...runtime.device };
  const nextDevice = { ...oldDevice };

  const coFields = [
    "carbon_monoxide",
    "co_alarm",
    "co",
    "last_seen",
    "linkquality",
    "availability",
  ];

  for (const field of coFields) {
    if (data[field] !== undefined) {
      nextDevice[field] = data[field];
    }
  }

  if (
    String(oldDevice.type || "unknown").trim() === "unknown"
  ) {
    nextDevice.type = "carbon_monoxide";
  }

  const oldAlarmActive =
    isActiveSignal(oldDevice.carbon_monoxide) ||
    isActiveSignal(oldDevice.co_alarm);
  const nextAlarmActive =
    isActiveSignal(nextDevice.carbon_monoxide) ||
    isActiveSignal(nextDevice.co_alarm);
  const alarmActiveChanged =
    oldAlarmActive !== nextAlarmActive;

  const carbonMonoxideNeedsPersist =
    data.carbon_monoxide !== undefined &&
    data.carbon_monoxide !==
      runtime.persistedCarbonMonoxide;
  const coAlarmNeedsPersist =
    data.co_alarm !== undefined &&
    data.co_alarm !== runtime.persistedCoAlarm;
  const alarmStateNeedsPersist =
    carbonMonoxideNeedsPersist || coAlarmNeedsPersist;

  const coValueChanged = telemetryValueChanged(
    runtime.persistedCo,
    data.co,
  );
  const coPersistDue =
    coValueChanged &&
    (
      runtime.lastCoPersistAt === 0 ||
      now - runtime.lastCoPersistAt >=
        CO_VALUE_PERSIST_INTERVAL_MS ||
      alarmStateNeedsPersist
    );

  const telemetryPersistDue =
    runtime.lastTelemetryPersistAt === 0 ||
    now - runtime.lastTelemetryPersistAt >=
      CO_TELEMETRY_PERSIST_INTERVAL_MS;

  const updateData = {};

  // OFF → ON và ON → OFF luôn ghi ngay lập tức.
  if (carbonMonoxideNeedsPersist) {
    updateData.carbon_monoxide =
      data.carbon_monoxide;
  }

  if (coAlarmNeedsPersist) {
    updateData.co_alarm = data.co_alarm;
  }

  // ppm chỉ ghi tối đa 30 giây/lần và chỉ khi giá trị thay đổi.
  if (coPersistDue) {
    updateData.co = data.co;
  }

  // last_seen/linkquality chỉ ghi tối đa 60 giây/lần.
  if (telemetryPersistDue) {
    if (data.last_seen !== undefined) {
      updateData.last_seen = data.last_seen;
    }

    if (data.linkquality !== undefined) {
      updateData.linkquality = data.linkquality;
    }

    if (data.availability !== undefined) {
      updateData.availability = data.availability;
    }
  }

  if (
    oldDevice.type === "unknown" ||
    oldDevice.type === undefined ||
    oldDevice.type === null
  ) {
    updateData.type = "carbon_monoxide";
  }

  if (alarmActiveChanged) {
    updateData.last_event = now;
  }

  runtime.device = nextDevice;

  if (Object.keys(updateData).length === 0) {
    return;
  }

  updateData.updated_at = now;
  const latestHomeFromCache = applyDeviceUpdateToLocalCache(
    uid,
    homeId,
    deviceId,
    updateData,
  );

  if (firebaseConnected) {
    try {
      await deviceRef.update(updateData);
    } catch (error) {
      console.log(
        "📴 CO UPDATE QUEUED:",
        deviceId,
        error.message,
      );
      enqueueOfflineFirebaseUpdate(
        `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
        updateData,
      );
    }
  } else {
    enqueueOfflineFirebaseUpdate(
      `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
      updateData,
    );
  }

  runtime.device = {
    ...runtime.device,
    ...updateData,
  };

  if (updateData.carbon_monoxide !== undefined) {
    runtime.persistedCarbonMonoxide =
      updateData.carbon_monoxide;
  }

  if (updateData.co_alarm !== undefined) {
    runtime.persistedCoAlarm = updateData.co_alarm;
  }

  if (updateData.co !== undefined) {
    runtime.persistedCo = updateData.co;
    runtime.lastCoPersistAt = now;
  }

  if (
    updateData.last_seen !== undefined ||
    updateData.linkquality !== undefined ||
    updateData.availability !== undefined
  ) {
    runtime.persistedLastSeen = updateData.last_seen ??
      runtime.persistedLastSeen;
    runtime.persistedLinkquality = updateData.linkquality ??
      runtime.persistedLinkquality;
    runtime.lastTelemetryPersistAt = now;
  }

  console.log("☠️ CO FIREBASE UPDATE:", deviceId, updateData);

  // Chỉ khi trạng thái an toàn/nguy hiểm thực sự đổi mới đọc dữ liệu Home,
  // tạo lịch sử và chạy Alarm cho tất cả thành viên.
  if (!alarmActiveChanged) {
    return;
  }

  const deviceName =
    runtime.device.name || oldDevice.name || deviceId;
  const latestCo = Number(
    nextDevice.co ?? oldDevice.co,
  );
  const coText = Number.isFinite(latestCo)
    ? ` (${latestCo} ppm)`
    : "";

  if (firebaseConnected) {
    await addDeviceNotification(
      uid,
      homeId,
      deviceId,
      nextAlarmActive
        ? `Phát hiện khí CO${coText}`
        : `Khí CO đã trở lại bình thường${coText}`,
      "status",
    );
  }

  // Khi CO hết nguy hiểm, đóng ngay các incident CO tương ứng và dừng còi
  // vật lý; không chờ auto-expire 30 phút.
  if (!nextAlarmActive) {
    const latestHomeData =
      latestHomeFromCache ||
      getCachedHomeData(uid, homeId) ||
      {};

    if (firebaseConnected) {
      try {
        await resolveClearedPersistentEmergencyIncidents(
          uid,
          homeId,
          {
            homeOverride: latestHomeData,
            reason: "carbon_monoxide_cleared",
          },
        );
      } catch (error) {
        console.log(
          "CO INCIDENT RESOLVE DEFERRED:",
          homeId,
          error.message,
        );
      }
    }

    await reconcileOfflineAlarmDemandsForHome(uid, homeId);
    return;
  }

  const latestHomeData =
    latestHomeFromCache ||
    getCachedHomeData(uid, homeId) ||
    {};
  const homeName = latestHomeData.name || homeId;

  // processScheduleAlarmsForOwner cần state cũ để nhận đúng cạnh OFF → ON.
  const homeDataForAlarm = {
    ...latestHomeData,
    devices: {
      ...(latestHomeData.devices || {}),
      [deviceId]: oldDevice,
    },
  };

  const alarmReceiverUids = getAlarmReceiverUidsForHome(
    uid,
    homeId,
  );

  for (const receiverUid of alarmReceiverUids) {
    try {
      await processScheduleAlarmsForOwner(
        receiverUid,
        uid,
        homeId,
        homeName,
        deviceId,
        deviceName,
        "carbon_monoxide",
        homeDataForAlarm,
        {
          carbon_monoxide: nextDevice.carbon_monoxide,
          co_alarm: nextDevice.co_alarm,
          co: nextDevice.co,
          last_event: now,
          updated_at: now,
        },
      );
    } catch (receiverError) {
      console.log(
        "CO ALARM RECEIVER ERROR:",
        receiverUid,
        uid,
        homeId,
        receiverError.message,
      );
    }
  }

  await reconcileOfflineAlarmDemandsForHome(uid, homeId);
}

async function enqueueCarbonMonoxidePacket(
  deviceId,
  uid,
  homeId,
  data,
) {
  const previous =
    coSensorProcessingPromiseMap.get(deviceId) ||
    Promise.resolve();

  const current = previous
    .catch(() => { })
    .then(() => {
      return processCarbonMonoxidePacket(
        deviceId,
        uid,
        homeId,
        data,
      );
    });

  coSensorProcessingPromiseMap.set(deviceId, current);

  try {
    await current;
  } finally {
    if (
      coSensorProcessingPromiseMap.get(deviceId) === current
    ) {
      coSensorProcessingPromiseMap.delete(deviceId);
    }
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
        const devicesSnap = await db
          .ref(`accounts/${uid}/homes/${homeId}/devices`)
          .once("value");

        const devices = devicesSnap.val() || {};
        const sameTypeDevices = Object.values(devices).filter((device) => {
          return device?.type === deviceType;
        });

        let defaultBaseName = "Thiết bị chưa nhận diện";

        switch (deviceType) {
          case "door":
            defaultBaseName = "Cửa Nhà";
            break;

          case "window":
            defaultBaseName = "Cửa Sổ";
            break;

          case "gate":
            defaultBaseName = "Cổng Nhà";
            break;

          case "door_lock":
          case "lock":
            defaultBaseName = "Khóa Cửa";
            break;

          case "motion":
            defaultBaseName = "Giám sát chuyển động";
            break;

          case "presence":
            defaultBaseName = "Giám sát hiện diện";
            break;

          case "vibration":
            defaultBaseName = "Ghi nhận rung chấn";
            break;

          case "glass_break":
            defaultBaseName = "Phát hiện kính vỡ";
            break;

          case "smoke":
            defaultBaseName = "Báo cháy";
            break;

          case "heat":
            defaultBaseName = "Cảnh báo nhiệt độ";
            break;

          case "carbon_monoxide":
            defaultBaseName = "Cảnh báo khí CO";
            break;

          case "gas":
            defaultBaseName = "Cảnh báo rò rỉ gas";
            break;

          case "water_leak":
          case "flood":
            defaultBaseName = "Cảnh báo ngập nước";
            break;

          case "temperature":
            defaultBaseName = "Nhiệt độ và độ ẩm";
            break;

          case "sos":
            defaultBaseName = "Nút SOS";
            break;

          case "smart_plug":
            defaultBaseName = "Ổ cắm thông minh";
            break;

          case "power_monitor":
            defaultBaseName = "Theo dõi điện năng";
            break;

          case "ups":
            defaultBaseName = "Nguồn dự phòng";
            break;

          case "siren":
            defaultBaseName = "Còi báo động";
            break;

          case "smart_valve":
            defaultBaseName = "Van nước thông minh";
            break;

          case "camera":
            defaultBaseName = "Camera";
            break;

          case "doorbell":
            defaultBaseName = "Chuông cửa";
            break;

          case "keypad":
            defaultBaseName = "Bàn phím an ninh";
            break;

          case "repeater":
            defaultBaseName = "Bộ mở rộng sóng";
            break;
        }

        // Thiết bị đầu tiên không có số. Từ thiết bị thứ hai mới dùng 2, 3...
        // Đồng thời tránh trùng tên nếu một thiết bị cũ đã bị xóa hoặc đổi tên.
        let defaultName = defaultBaseName;

        if (sameTypeDevices.length > 0) {
          const existingNames = new Set(
            sameTypeDevices.map((device) => {
              return String(device?.name || "").trim();
            }),
          );

          let sequenceNumber = Math.max(2, sameTypeDevices.length + 1);

          while (existingNames.has(`${defaultBaseName} ${sequenceNumber}`)) {
            sequenceNumber += 1;
          }

          defaultName = `${defaultBaseName} ${sequenceNumber}`;
        }
        await db.ref(`accounts/${uid}/homes/${homeId}/devices/${ieee}`).set({
          name: defaultName,
          ieee,
          type: deviceType,
          roomId: roomId || "unassigned",
          // `alarm` là lịch Map đối với sensor an ninh, nhưng là trạng thái
          // boolean đối với còi. Chỉ ghi đúng kiểu theo loại thiết bị.
          alarm:
            deviceType === "siren"
              ? null
              : isSecurityDeviceType(deviceType)
                ? {
                    enabled: true,
                    start: "23:00",
                    end: "06:00",
                    repeatMinutes: 0,
                    days: [1, 2, 3, 4, 5, 6, 7],
                  }
                : null,
          availability: "unknown",
          last_seen: null,

          battery: null,
          battery_low: null,
          battpercentage: null,
          voltage: null,
          linkquality: null,

          contact: null,
          smoke: null,
          tamper: false,
          temperature: null,
          humidity: null,
          action: null,

          // Cảm biến chuyển động / rung.
          occupancy: null,
          motion: null,
          vibration: null,
          vibration_strength: null,
          last_vibration_at: null,
          vibration_active_until: null,
          glass_break: null,
          broken_glass: null,
          sensitivity: null,

          // Cảm biến khí CO.
          carbon_monoxide: null,
          co: null,

          // Còi báo động. Bước này chỉ lưu cấu hình nhận được,
          // chưa tự động gửi lệnh bật còi.
          melody: null,
          duration: null,
          volume: null,
          last_siren_report_at: null,

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
        scheduleLocalRuntimeSnapshotSave();

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

    // Không coi chính lệnh điều khiển /set hoặc /get là trạng thái sensor.
    // Chỉ lưu trạng thái thực do Zigbee2MQTT publish lại ở topic thiết bị.
    if (subTopic === "set" || subTopic === "get") {
      return;
    }

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

    // Fast path riêng cho cảm biến CO: giữ Alarm tức thời nhưng không đọc/ghi
    // Firebase theo từng packet MQTT liên tục.
    if (isCarbonMonoxidePayload(data)) {
      await enqueueCarbonMonoxidePacket(
        deviceId,
        uid,
        homeId,
        data,
      );
      return;
    }

    const deviceRef = db.ref(
      `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
    );

    let homeData = getCachedHomeData(uid, homeId);
    let oldData = homeData?.devices?.[deviceId] || null;

    if ((!homeData || !oldData) && firebaseConnected) {
      try {
        const [oldSnap, homeSnap] = await Promise.all([
          deviceRef.once("value"),
          db.ref(`accounts/${uid}/homes/${homeId}`)
            .once("value"),
        ]);

        oldData = oldSnap.val() || oldData || {};
        homeData = homeSnap.val() || homeData || {};
      } catch (error) {
        console.log(
          "MQTT FIREBASE READ FALLBACK:",
          deviceId,
          error.message,
        );
      }
    }

    if (!homeData || !oldData) {
      console.log(
        "⚠️ SENSOR SKIPPED, NO LOCAL SNAPSHOT:",
        deviceId,
      );
      return;
    }

    const deviceName = oldData.name || deviceId;
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
      "glass_break",
      "broken_glass",
      "sensitivity",
      "angle",
      "x_axis",
      "y_axis",
      "z_axis",
      "gas",
      "gas_alarm",
      "carbon_monoxide",
      "co_alarm",
      "co",
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

      // Cấu hình của còi NEO NAS-AB02B2. Trạng thái `alarm` được
      // copy riêng sau khi đã chắc chắn đây là thiết bị còi, tránh ghi đè
      // Map lịch Alarm của sensor an ninh.
      "melody",
      "duration",
      "volume",
      "battpercentage",
    ];

    for (const field of fieldsToCopy) {
      if (data[field] !== undefined) {
        updateData[field] = data[field];
      }
    }

    const provisionalDeviceType =
      updateData.type ||
      currentDeviceType ||
      inferredDeviceType ||
      "unknown";

    if (
      provisionalDeviceType === "siren" &&
      data.alarm !== undefined
    ) {
      updateData.alarm = data.alarm;
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
      "glass_break",
      "broken_glass",
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
      "alarm",
    ];

    const hasChangedEventField = eventFields.some((field) => {
      if (field === "alarm" && provisionalDeviceType !== "siren") {
        return false;
      }

      return (
        data[field] !== undefined &&
        data[field] !== oldData[field]
      );
    });

    const vibrationEventPacket =
      provisionalDeviceType === "vibration" &&
      (
        (
          isActiveSignal(data.vibration) &&
          !isActiveSignal(oldData.vibration)
        ) ||
        (
          isVibrationAction(data.action) &&
          (
            !isVibrationAction(oldData.action) ||
            now - Number(oldData.last_vibration_at || 0) >
              VIBRATION_ACTIVE_WINDOW_MS
          )
        )
      );
    const glassBreakEventPacket =
      provisionalDeviceType === "glass_break" &&
      (
        (
          (
            isActiveSignal(data.glass_break) ||
            isActiveSignal(data.broken_glass)
          ) &&
          !(
            isActiveSignal(oldData.glass_break) ||
            isActiveSignal(oldData.broken_glass)
          )
        ) ||
        (
          isGlassBreakAction(data.action) &&
          !isGlassBreakAction(oldData.action)
        )
      );
    const vibrationClearPacket =
      provisionalDeviceType === "vibration" &&
      data.vibration !== undefined &&
      !isActiveSignal(data.vibration) &&
      !isVibrationAction(data.action);
    const shouldRecordLastEvent =
      provisionalDeviceType === "vibration"
        ? vibrationEventPacket
        : provisionalDeviceType === "glass_break"
          ? glassBreakEventPacket
          : hasChangedEventField && !vibrationClearPacket;

    if (shouldRecordLastEvent) {
      updateData.last_event = now;
    }

    if (data.battery !== undefined) {
      updateData.battery_status = "percent";
    }

    // Còi NEO dùng battpercentage thay cho battery. Chuẩn hóa thêm về
    // battery để phần UI/kiểm tra pin hiện có dùng chung được ngay.
    if (
      data.battpercentage !== undefined &&
      data.battery === undefined
    ) {
      const normalizedBattery = Number(
        data.battpercentage,
      );

      if (Number.isFinite(normalizedBattery)) {
        updateData.battery = normalizedBattery;
        updateData.battery_status = "percent";
      }
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

    if (resolvedDeviceType === "vibration") {
      if (vibrationEventPacket) {
        updateData.last_event = now;
        updateData.last_vibration_at = now;
        updateData.vibration_active_until =
          now + VIBRATION_ACTIVE_WINDOW_MS;
      } else if (
        data.vibration !== undefined &&
        !isActiveSignal(data.vibration)
      ) {
        updateData.vibration_active_until = null;
      }
    }

    if (
      resolvedDeviceType === "siren" &&
      data.alarm !== undefined
    ) {
      // Chỉ timestamp packet trạng thái thật do Zigbee2MQTT trả về.
      // Không dùng updated_at vì telemetry khác có thể làm xác nhận OFF sai.
      updateData.last_siren_report_at = now;
    }

    const latestHomeFromCache = applyDeviceUpdateToLocalCache(
      uid,
      homeId,
      deviceId,
      updateData,
    );

    if (firebaseConnected) {
      try {
        await deviceRef.update(updateData);
      } catch (error) {
        console.log(
          "📴 DEVICE UPDATE QUEUED:",
          deviceId,
          error.message,
        );
        enqueueOfflineFirebaseUpdate(
          `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
          updateData,
        );
      }
    } else {
      enqueueOfflineFirebaseUpdate(
        `accounts/${uid}/homes/${homeId}/devices/${deviceId}`,
        updateData,
      );
    }

    if (
      resolvedDeviceType === "vibration" &&
      updateData.last_vibration_at === now
    ) {
      scheduleVibrationStateClear(
        uid,
        homeId,
        deviceId,
        now,
      );
    } else if (
      resolvedDeviceType === "vibration" &&
      data.vibration !== undefined &&
      !isActiveSignal(data.vibration)
    ) {
      cancelVibrationStateClear(uid, homeId, deviceId);
    }

    // Còi có duration hữu hạn. Nếu nó tự OFF trong khi Home vẫn còn incident
    // yêu cầu còi, bật lại ngay thay vì chờ chu kỳ refresh 20 phút.
    if (
      resolvedDeviceType === "siren" &&
      data.alarm !== undefined &&
      !isActiveSignal(data.alarm)
    ) {
      const sirenRuntime = homeSirenRuntimeMap.get(
        getHomeSirenRuntimeKey(uid, homeId),
      );

      if (sirenRuntime?.desiredOn === true) {
        setTimeout(() => {
          void setPhysicalSirenForHome(
            uid,
            homeId,
            true,
            {
              force: true,
              reason: "siren_reported_off_while_incident_active",
            },
          );
        }, 1000);
      }
    }

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

    const latestHomeData = latestHomeFromCache || {
      ...homeData,
      devices: {
        ...(homeData.devices || {}),
        [deviceId]: {
          ...oldData,
          ...updateData,
        },
      },
    };

    if (incidentStateChanged && firebaseConnected) {
      try {
        await validateSecurityIncidentsForHome(
          uid,
          homeId,
          "device_state_changed",
          { homeOverride: latestHomeData },
        );
      } catch (error) {
        console.log(
          "SECURITY INCIDENT VALIDATION DEFERRED:",
          homeId,
          error.message,
        );
      }
    }

    if (
      updateData.last_event !== undefined &&
      isPersistentEmergencyIncidentItem({
        type: resolvedDeviceType,
      }) &&
      isEmergencyIncidentItemStillUnsafe(
        latestHomeData,
        {
          deviceId,
          type: resolvedDeviceType,
        },
      ) === false
    ) {
      if (firebaseConnected) {
        try {
          await resolveClearedPersistentEmergencyIncidents(
            uid,
            homeId,
            {
              homeOverride: latestHomeData,
              reason: `${resolvedDeviceType}_cleared`,
            },
          );
        } catch (error) {
          console.log(
            "EMERGENCY INCIDENT RESOLVE DEFERRED:",
            homeId,
            error.message,
          );
        }
      }
    }


    if (
      updateData.last_event !== undefined &&
      firebaseConnected
    ) {
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
        const latestCo = Number(
          updateData.co ?? oldData.co,
        );
        const coText = Number.isFinite(latestCo)
          ? ` (${latestCo} ppm)`
          : "";

        statusText = isActiveSignal(
          updateData.carbon_monoxide ??
          updateData.co_alarm ??
          oldData.carbon_monoxide ??
          oldData.co_alarm,
        )
          ? `Phát hiện khí CO${coText}`
          : `Khí CO đang bình thường${coText}`;
      } else if (currentType === "siren") {
        statusText = isActiveSignal(
          updateData.alarm ?? oldData.alarm,
        )
          ? "Còi báo động đang bật"
          : "Còi báo động đã tắt";
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

    if (
      firebaseConnected &&
      updateData.tamper !== undefined &&
      updateData.tamper !== oldTamper
    ) {
      await addDeviceNotification(
        uid,
        homeId,
        deviceId,
        updateData.tamper ? "Tamper detected" : "Tamper cleared",
        "tamper",
      );
    }

    console.log("📡 UPDATE:", deviceId, updateData);

    // ===== HOME ALARM RECEIVERS =====
    // Chủ nhà + sharedByHome/{homeId}; mỗi receiver được xử lý độc lập.
    const alarmReceiverUids = getAlarmReceiverUidsForHome(
      uid,
      homeId,
    );

    console.log(
      "🚨 HOME ALARM RECEIVERS:",
      homeId,
      alarmReceiverUids,
    );

    for (const receiverUid of alarmReceiverUids) {
      try {
        await processScheduleAlarmsForOwner(
          receiverUid,
          uid,
          homeId,
          homeName,
          deviceId,
          deviceName,
          updateData.type || oldData.type || "door",
          homeData,
          updateData,
        );
      } catch (receiverError) {
        console.log(
          "HOME ALARM RECEIVER ERROR:",
          receiverUid,
          uid,
          homeId,
          receiverError.message,
        );
      }
    }

    await reconcileOfflineAlarmDemandsForHome(
      uid,
      homeId,
    );
  } catch (err) {
    console.log("MQTT ERROR:", err.message);
  }
});
