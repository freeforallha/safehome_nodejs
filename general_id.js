const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { execFileSync, execSync } = require("child_process");

const SOURCE_DIR = "/home/pi/safehome_nodejs";
const RUNTIME_DIR = "/opt/safehome-node";
const Z2M_DIR = "/home/pi/zigbee2mqtt";
const Z2M_DATA = path.join(Z2M_DIR, "data");
const SERVICE_ACCOUNT = path.join(RUNTIME_DIR, "serviceAccount.json");
const Z2M_CONFIG = path.join(Z2M_DATA, "configuration.yaml");
const Z2M_SECRET = path.join(Z2M_DATA, "secret.yaml");
const TIMEOUT = 5000;

// Khóa dẫn xuất nội bộ của SafeHome.
// Token phụ thuộc vào serial riêng của từng Pi nên mỗi Hub có token khác nhau.
// Không thay đổi giá trị này sau khi đã triển khai sản phẩm.
const Z2M_TOKEN_DERIVATION_KEY =
  "safehome-z2m-auth-v1-8f6c2b9d4e7a1c53";

const warnings = [];

function run(command, args = [], timeout = TIMEOUT) {
  try {
    return execFileSync(command, args, {
      encoding: "utf8",
      timeout,
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return "";
  }
}

function runShell(command, timeout = TIMEOUT) {
  try {
    return execSync(command, {
      encoding: "utf8",
      timeout,
      stdio: ["ignore", "pipe", "ignore"],
      shell: "/bin/bash",
    }).trim();
  } catch {
    return "";
  }
}

function readText(filePath) {
  try {
    return fs.readFileSync(filePath, "utf8");
  } catch {
    return "";
  }
}

function readJson(filePath) {
  const raw = readText(filePath);
  if (!raw) return null;

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function sha256Short(value, length = 16) {
  if (!value) return "";
  return crypto.createHash("sha256").update(value).digest("hex").slice(0, length);
}

function fileFingerprint(filePath) {
  try {
    return sha256Short(fs.readFileSync(filePath));
  } catch {
    return "";
  }
}

function statInfo(filePath) {
  const raw = run("stat", ["-c", "%A|%a|%U|%G|%s|%y", filePath]);
  if (!raw) return { exists: false, path: filePath };

  const [permissions, mode, owner, group, size, modifiedAt] = raw.split("|");
  return {
    exists: true,
    path: filePath,
    permissions,
    mode,
    owner,
    group,
    size_bytes: Number(size) || 0,
    modified_at: modifiedAt || "",
  };
}

function getPiSerial() {
  const cpuInfo = readText("/proc/cpuinfo");
  return cpuInfo.match(/Serial\s*:\s*(.+)/)?.[1]?.trim() || "unknown_serial";
}

function generateZigbeeToken(piSerial) {
  if (!piSerial || piSerial === "unknown_serial") {
    throw new Error("Không đọc được Pi serial nên không thể tạo token.");
  }

  return crypto
    .createHmac("sha256", Z2M_TOKEN_DERIVATION_KEY)
    .update(`safehome-hub:${piSerial}`)
    .digest("base64url");
}

function getStoredZigbeeToken() {
  const secret = readText(Z2M_SECRET);

  return (
    secret.match(
      /^\s*auth_token\s*:\s*["']?([^"'\r\n]+)["']?\s*$/m,
    )?.[1]?.trim() || ""
  );
}

function writeZigbeeToken(token) {
  if (!fs.existsSync(Z2M_DATA)) {
    throw new Error(`Không tìm thấy thư mục: ${Z2M_DATA}`);
  }

  const original = readText(Z2M_SECRET);
  const tokenLine = `auth_token: "${token}"`;

  let updated;
  if (/^\s*auth_token\s*:/m.test(original)) {
    updated = original.replace(/^\s*auth_token\s*:.*$/m, tokenLine);
  } else {
    const prefix =
      original && !original.endsWith("\n")
        ? `${original}\n`
        : original;

    updated = `${prefix}${tokenLine}\n`;
  }

  const currentToken = getStoredZigbeeToken();
  if (currentToken === token) {
    return { changed: false, path: Z2M_SECRET };
  }

  const tempPath = `${Z2M_SECRET}.tmp-${process.pid}`;

  fs.writeFileSync(tempPath, updated, {
    encoding: "utf8",
    mode: 0o600,
  });

  fs.chmodSync(tempPath, 0o600);
  fs.renameSync(tempPath, Z2M_SECRET);
  fs.chmodSync(Z2M_SECRET, 0o600);

  if (
    typeof process.geteuid === "function" &&
    process.geteuid() === 0
  ) {
    run("chown", ["pi:pi", Z2M_SECRET]);
  }

  return { changed: true, path: Z2M_SECRET };
}

function syncZigbeeToken(piSerial, restartService = true) {
  const expectedToken = generateZigbeeToken(piSerial);
  const writeResult = writeZigbeeToken(expectedToken);

  let restartResult = "not_requested";

  if (restartService) {
    run(
      "systemctl",
      ["restart", "zigbee2mqtt.service"],
      15000,
    );
    restartResult = "requested";
  }

  return {
    hub_id: `dev_${sha256Short(piSerial, 16)}`,
    token_fingerprint: sha256Short(expectedToken),
    token_changed: writeResult.changed,
    secret_path: writeResult.path,
    zigbee2mqtt_restart: restartResult,
  };
}

function getPiModel() {
  return readText("/proc/device-tree/model").replace(/\0/g, "").trim() || "unknown";
}

function getCpuTemperature() {
  const value = Number.parseInt(readText("/sys/class/thermal/thermal_zone0/temp"), 10);
  return Number.isFinite(value) ? Math.round((value / 1000) * 10) / 10 : null;
}

function getMemory() {
  const total = os.totalmem();
  const free = os.freemem();
  const used = total - free;

  return {
    total_bytes: total,
    used_bytes: used,
    free_bytes: free,
    used_percent: total ? Math.round((used / total) * 1000) / 10 : null,
  };
}

function getDisk(target = "/") {
  const lines = run("df", ["-P", target]).split("\n");
  if (lines.length < 2) return {};

  const parts = lines.at(-1).trim().split(/\s+/);
  return {
    total_bytes: Number(parts[1]) * 1024 || null,
    used_bytes: Number(parts[2]) * 1024 || null,
    available_bytes: Number(parts[3]) * 1024 || null,
    used_percent: Number.parseInt(parts[4], 10) || null,
  };
}

function getInterfaces() {
  const result = {};

  for (const [name, entries] of Object.entries(os.networkInterfaces())) {
    const addresses = (entries || [])
      .filter((item) => !item.internal)
      .map((item) => ({
        family: item.family,
        address: item.address,
        cidr: item.cidr || "",
        mac: item.mac,
      }));

    if (addresses.length) result[name] = addresses;
  }

  return result;
}

function getDefaultRoute() {
  const raw = run("ip", ["route", "show", "default"]);
  const line = raw.split("\n").find(Boolean) || "";

  return {
    gateway: line.match(/\bvia\s+(\S+)/)?.[1] || "",
    interface: line.match(/\bdev\s+(\S+)/)?.[1] || "",
    raw: line,
  };
}

function getInternetStatus() {
  return {
    dns_resolved: Boolean(run("getent", ["ahostsv4", "firebase.google.com"], 4000)),
    https_outbound: Boolean(
      run("curl", ["-fsSI", "--max-time", "4", "https://firebase.google.com"], 6000),
    ),
  };
}

function systemctlValue(service, property) {
  return run("systemctl", ["show", service, `--property=${property}`, "--value"]);
}

function serviceInfo(service) {
  const pid = Number.parseInt(systemctlValue(service, "MainPID"), 10);
  const restarts = Number.parseInt(systemctlValue(service, "NRestarts"), 10);

  return {
    load_state: systemctlValue(service, "LoadState") || "unknown",
    active_state: systemctlValue(service, "ActiveState") || "unknown",
    sub_state: systemctlValue(service, "SubState") || "unknown",
    enabled_state: systemctlValue(service, "UnitFileState") || "unknown",
    user: systemctlValue(service, "User") || "",
    group: systemctlValue(service, "Group") || "",
    pid: Number.isFinite(pid) ? pid : 0,
    restart_count: Number.isFinite(restarts) ? restarts : 0,
    working_directory: systemctlValue(service, "WorkingDirectory") || "",
    started_at: systemctlValue(service, "ExecMainStartTimestamp") || "",
  };
}

function parseYamlBlock(text, blockName) {
  const lines = text.split(/\r?\n/);
  const start = lines.findIndex((line) => line.trim() === `${blockName}:`);
  if (start < 0) return "";

  const block = [];
  for (let i = start + 1; i < lines.length; i++) {
    const line = lines[i];
    if (/^\S/.test(line) && line.trim()) break;
    block.push(line);
  }

  return block.join("\n");
}

function yamlValue(text, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = text.match(new RegExp(`^\\s*${escaped}\\s*:\\s*(.+?)\\s*$`, "m"));
  return (match?.[1] || "").trim().replace(/^['"]|['"]$/g, "");
}

function zigbeeUsb() {
  const directory = "/dev/serial/by-id";

  try {
    const devices = fs.readdirSync(directory).map((name) => {
      const idPath = path.join(directory, name);
      let resolvedPath = "";
      try {
        resolvedPath = fs.realpathSync(idPath);
      } catch {}

      return { name, id_path: idPath, resolved_path: resolvedPath };
    });

    return { connected: devices.length > 0, devices };
  } catch {
    return { connected: false, devices: [] };
  }
}

function zigbeeDeviceCount() {
  const raw = readText(path.join(Z2M_DATA, "database.db"));
  if (!raw) return null;
  return raw.split(/\r?\n/).filter((line) => line.trim()).length;
}

function listeningSockets() {
  return run("ss", ["-lntup"])
    .split("\n")
    .slice(1)
    .filter(Boolean)
    .map((line) => {
      const columns = line.trim().split(/\s+/);
      const localAddress = columns[4] || "";
      const processMatch = line.match(/users:\(\("([^"]+)".*pid=(\d+)/);
      const portMatch = localAddress.match(/:(\d+)$/);

      return {
        protocol: columns[0] || "",
        state: columns[1] || "",
        local_address: localAddress,
        port: portMatch ? Number(portMatch[1]) : null,
        process: processMatch?.[1] || "",
        pid: processMatch ? Number(processMatch[2]) : null,
        public:
          localAddress.startsWith("0.0.0.0:") ||
          localAddress.startsWith("[::]:") ||
          localAddress.startsWith("*:")
      };
    });
}

function tailscaleInfo() {
  const raw = run("tailscale", ["status", "--json"]);
  if (!raw) {
    return {
      installed: Boolean(run("which", ["tailscale"])),
      active: false,
    };
  }

  try {
    const parsed = JSON.parse(raw);
    const self = parsed.Self || {};
    const ips = Array.isArray(self.TailscaleIPs) ? self.TailscaleIPs : [];

    return {
      installed: true,
      active: parsed.BackendState === "Running",
      hostname: self.HostName || "",
      dns_name: self.DNSName || "",
      ipv4: ips.find((ip) => ip.includes(".")) || "",
      ipv6: ips.find((ip) => ip.includes(":")) || "",
      online: self.Online === true,
    };
  } catch {
    return { installed: true, active: false };
  }
}

function firewallInfo() {
  const ufw = runShell("ufw status 2>/dev/null");
  const nft = runShell("nft list ruleset 2>/dev/null");

  return {
    ufw_installed: Boolean(run("which", ["ufw"])),
    ufw_status: ufw.split("\n")[0] || "not available",
    nftables_rules_present: Boolean(nft),
  };
}

function gitInfo() {
  return {
    source_directory: SOURCE_DIR,
    branch: run("git", ["-C", SOURCE_DIR, "branch", "--show-current"]),
    commit: run("git", ["-C", SOURCE_DIR, "rev-parse", "--short", "HEAD"]),
    dirty: Boolean(run("git", ["-C", SOURCE_DIR, "status", "--porcelain"])),
  };
}

function firebaseInfo() {
  const account = readJson(SERVICE_ACCOUNT);

  return {
    configured: Boolean(account),
    project_id: account?.project_id || "",
    client_email: account?.client_email || "",
    file: statInfo(SERVICE_ACCOUNT),
    file_fingerprint: fileFingerprint(SERVICE_ACCOUNT),
    private_key_present: Boolean(account?.private_key),
    private_key_exposed: false,
  };
}

function zigbeeInfo(expectedToken) {
  const config = readText(Z2M_CONFIG);
  const frontend = parseYamlBlock(config, "frontend");
  const serial = parseYamlBlock(config, "serial");
  const advanced = parseYamlBlock(config, "advanced");
  const token = getStoredZigbeeToken();
  const tokenRef = yamlValue(frontend, "auth_token");
  const packageJson = readJson(path.join(Z2M_DIR, "package.json"));

  return {
    version: packageJson?.version || "",
    service: serviceInfo("zigbee2mqtt.service"),
    usb: zigbeeUsb(),
    coordinator: {
      configured_port: yamlValue(serial, "port"),
      adapter: yamlValue(serial, "adapter"),
      channel: yamlValue(advanced, "channel"),
    },
    joined_device_records: zigbeeDeviceCount(),
    frontend: {
      enabled: yamlValue(frontend, "enabled") === "true",
      port: Number.parseInt(yamlValue(frontend, "port"), 10) || null,
      auth_configured: Boolean(tokenRef && token),
      auth_reference: tokenRef,
      token_fingerprint: token ? sha256Short(token) : "",
      expected_token_fingerprint: expectedToken
        ? sha256Short(expectedToken)
        : "",
      token_matches_this_hub: Boolean(
        token && expectedToken && token === expectedToken,
      ),
      token_exposed: false,
      secret_file: statInfo(Z2M_SECRET),
    },
  };
}

function backendInfo() {
  const service = serviceInfo("safehome-node.service");
  const runtimeDirectory = service.working_directory || RUNTIME_DIR;

  return {
    service,
    node_version: process.version,
    runtime_directory: runtimeDirectory,
    exec_start: systemctlValue("safehome-node.service", "ExecStart"),
    runtime_index: statInfo(path.join(runtimeDirectory, "index.js")),
    source_git: gitInfo(),
    firebase: firebaseInfo(),
  };
}

function mqttInfo(sockets) {
  const listeners = sockets.filter((item) => item.port === 1883);

  return {
    service: serviceInfo("mosquitto.service"),
    listeners,
    public_listener: listeners.some((item) => item.public),
  };
}

const rawSerial = getPiSerial();
const hubId = `dev_${sha256Short(rawSerial, 16)}`;

let expectedZigbeeToken = "";
try {
  expectedZigbeeToken = generateZigbeeToken(rawSerial);
} catch {}

const argumentsSet = new Set(process.argv.slice(2));

if (argumentsSet.has("--show-token")) {
  if (!expectedZigbeeToken) {
    console.error(
      "Không thể tạo token vì không đọc được Pi serial.",
    );
    process.exit(1);
  }

  console.log(`Hub ID: ${hubId}`);
  console.log(`Zigbee2MQTT token: ${expectedZigbeeToken}`);
  process.exit(0);
}

if (argumentsSet.has("--sync-token")) {
  try {
    const result = syncZigbeeToken(
      rawSerial,
      !argumentsSet.has("--no-restart"),
    );

    console.log(JSON.stringify(result, null, 2));
    process.exit(0);
  } catch (error) {
    console.error(
      `Không thể đồng bộ token: ${error.message}`,
    );
    process.exit(1);
  }
}

const sockets = listeningSockets();
const backend = backendInfo();
const zigbee = zigbeeInfo(expectedZigbeeToken);
const mqtt = mqttInfo(sockets);
const firewall = firewallInfo();

if (!["safehome"].includes(backend.service.user)) {
  warnings.push(`Backend đang chạy bằng user không chuyên biệt: ${backend.service.user || "không xác định"}`);
}

if (!backend.firebase.file.exists || Number(backend.firebase.file.mode) > 640) {
  warnings.push("serviceAccount.json bị thiếu hoặc quyền file quá rộng");
}

if (
  backend.runtime_index.exists &&
  (backend.runtime_index.owner !== "root" || /w/.test(backend.runtime_index.permissions?.slice(4, 7) || ""))
) {
  warnings.push("Mã nguồn backend chưa ở chế độ root sở hữu và service chỉ đọc");
}

if (mqtt.public_listener) {
  warnings.push("MQTT đang mở ra ngoài localhost");
}

if (!zigbee.frontend.auth_configured) {
  warnings.push("Frontend Zigbee2MQTT chưa bật auth_token");
} else if (!zigbee.frontend.token_matches_this_hub) {
  warnings.push(
    "Token Zigbee2MQTT hiện tại không khớp token được tạo từ Pi này",
  );
}

if (firewall.ufw_status === "Status: inactive" && !firewall.nftables_rules_present) {
  warnings.push("Chưa có firewall host đang hoạt động");
}

const unexpectedPublicPorts = sockets
  .filter((item) => item.public && ![22, 8080].includes(item.port))
  .map((item) => item.port)
  .filter((item) => item !== null);

if (unexpectedPublicPorts.length) {
  warnings.push(`Có cổng public ngoài danh sách dự kiến: ${[...new Set(unexpectedPublicPorts)].join(", ")}`);
}

const healthyServices = [
  backend.service,
  zigbee.service,
  mqtt.service,
].every((service) => service.active_state === "active");

const result = {
  generated_at: new Date().toISOString(),
  overall_status:
    healthyServices && warnings.length === 0
      ? "healthy"
      : healthyServices
        ? "healthy_with_warnings"
        : "degraded",

  hub: {
    device_id: hubId,
    hostname: os.hostname(),
    pi_model: getPiModel(),
    cpu_serial: rawSerial,
    architecture: os.arch(),
    platform: os.platform(),
    kernel_release: os.release(),
  },

  system: {
    node_version: process.version,
    uptime_seconds: Math.floor(os.uptime()),
    cpu_temperature_c: getCpuTemperature(),
    cpu_count: os.cpus().length,
    load_average: os.loadavg(),
    memory: getMemory(),
    disk_root: getDisk("/"),
    timezone: run("timedatectl", ["show", "--property=Timezone", "--value"]),
    time_synchronized:
      run("timedatectl", ["show", "--property=NTPSynchronized", "--value"]) === "yes",
  },

  network: {
    interfaces: getInterfaces(),
    default_route: getDefaultRoute(),
    internet: getInternetStatus(),
    tailscale: tailscaleInfo(),
    firewall,
    listening_sockets: sockets,
  },

  services: {
    safehome_node: backend.service,
    zigbee2mqtt: zigbee.service,
    mosquitto: mqtt.service,
    tailscaled: serviceInfo("tailscaled.service"),
  },

  backend,
  zigbee,
  mqtt,

  security: {
    backend_runs_as_dedicated_user: backend.service.user === "safehome",
    backend_code_owned_by_root: backend.runtime_index.owner === "root",
    backend_code_group_writable: /w/.test(backend.runtime_index.permissions?.slice(4, 7) || ""),
    service_account_secure:
      backend.firebase.file.exists && Number(backend.firebase.file.mode) <= 640,
    mqtt_public: mqtt.public_listener,
    zigbee_frontend_auth_enabled: zigbee.frontend.auth_configured,
    zigbee_token_matches_this_hub:
      zigbee.frontend.token_matches_this_hub,
    secrets_are_masked: true,
  },

  token_management: {
    generation_basis: "Pi serial + SafeHome HMAC-SHA256",
    stored_token_matches_this_hub:
      zigbee.frontend.token_matches_this_hub,
    show_command:
      "node /home/pi/safehome_nodejs/general_id.js --show-token",
    sync_command:
      "sudo node /home/pi/safehome_nodejs/general_id.js --sync-token",
    automatic_sync_ready: true,
    token_exposed_in_report: false,
  },

  secret_fingerprints: {
    firebase_service_account_file: backend.firebase.file_fingerprint,
    zigbee_frontend_token: zigbee.frontend.token_fingerprint,
    note: "Chỉ là SHA-256 rút gọn để đối chiếu, không phải token thật.",
  },

  warnings,
};

if (!process.argv.includes("--json")) {
  console.log("\n🧠 SAFEHOME HUB DIAGNOSTIC REPORT\n");
}

console.log(JSON.stringify(result, null, 2));
