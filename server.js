const mqtt = require("mqtt");
const admin = require("firebase-admin");

// 🔥 Firebase Admin (bạn phải tải key từ Firebase Console)
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://YOUR_PROJECT.firebaseio.com"
});

// MQTT Zigbee2MQTT
const client = mqtt.connect("mqtt://localhost:1883");

client.on("connect", () => {
  console.log("MQTT connected");

  client.subscribe("zigbee2mqtt/#");
});

// 🔥 khi sensor gửi dữ liệu
client.on("message", (topic, message) => {
  const data = JSON.parse(message.toString());

  console.log(topic, data);

  const deviceName = topic.split("/")[1];

  const status = data.contact;   // cửa mở/đóng
  const tamper = data.tamper;

  // 🔴 nếu có bất thường
  if (status === false || tamper === true) {
    sendAlarm(deviceName);
  }
});

// 🔥 gửi push notification
function sendAlarm(device) {
  const payload = {
    notification: {
      title: "🚨 SafeHome Alarm",
      body: `${device} có bất thường!`
    }
  };

  admin.messaging().sendToTopic("all", payload)
    .then(() => console.log("Alarm sent"))
    .catch(err => console.log(err));
}
admin.messaging().send({
  token: deviceToken,
  notification: {
    title: "🚨 Cảnh báo",
    body: "Có cửa mở!"
  },
  android: {
    priority: "high"
  }
});
