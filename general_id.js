const fs = require("fs");
const crypto = require("crypto");
const { execSync } = require("child_process");

// ================= CPU SERIAL (HUB BASE ID) =================
function getPiSerial() {
  try {
    const cpuInfo = fs.readFileSync("/proc/cpuinfo", "utf8");
    const match = cpuInfo.match(/Serial\s*:\s*(.+)/);
    return match ? match[1].trim() : "unknown_serial";
  } catch {
    return "unknown_serial";
  }
}

const rawSerial = getPiSerial();

const HUB_ID =
  "dev_" +
  crypto.createHash("sha256").update(rawSerial).digest("hex").slice(0, 16);

// ================= ZIGBEE USB =================
function getZigbeeUSB() {
  try {
    const out = execSync("ls -l /dev/serial/by-id/", {
      encoding: "utf8",
    });
    return out.trim();
  } catch {
    return "NO_ZIGBEE_USB_FOUND";
  }
}

// ================= NODE VERSION =================
function getNodeVersion() {
  return process.version;
}

// ================= SYSTEM INFO =================
function getUptime() {
  try {
    return execSync("uptime -p").toString().trim();
  } catch {
    return "unknown";
  }
}

// ================= OUTPUT =================
const result = {
  hub: {
    device_id: HUB_ID,
    cpu_serial: rawSerial,
  },

  zigbee: {
    usb: getZigbeeUSB(),
  },

  system: {
    node_version: getNodeVersion(),
    uptime: getUptime(),
  },
};

console.log("\n🧠 SAFEHOME GENERAL ID REPORT\n");
console.log(JSON.stringify(result, null, 2));
