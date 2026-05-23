const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccount.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://YOUR_PROJECT.firebaseio.com",
});

const db = admin.database();

async function migrate() {
  const accountsSnap = await db.ref("accounts").once("value");

  if (!accountsSnap.exists()) {
    console.log("No accounts found");
    return;
  }

  const accounts = accountsSnap.val();

  for (const uid of Object.keys(accounts)) {
    const account = accounts[uid];

    const homes = account.homes || {};
    const sharedHomes = account.sharedHomes || {};

    // ===== 1. MIGRATE OWN HOMES =====
    for (const homeId of Object.keys(homes)) {
      const homeData = homes[homeId];

      await db.ref(`homes/${homeId}`).update({
        info: homeData,
        ownerUid: uid,
      });

      await db.ref(`homes/${homeId}/members/${uid}`).set("owner");

      await db.ref(`accounts/${uid}/joinedHomes/${homeId}`).set(true);
    }

    // ===== 2. MIGRATE SHARED HOMES =====
    for (const homeId of Object.keys(sharedHomes)) {
      const homeData = sharedHomes[homeId];

      const ownerUid = homeData._ownerUid || null;

      await db.ref(`homes/${homeId}`).update({
        info: homeData,
        ownerUid: ownerUid,
      });

      await db.ref(`homes/${homeId}/members/${uid}`).set("member");

      await db.ref(`accounts/${uid}/joinedHomes/${homeId}`).set(true);
    }
  }

  console.log("MIGRATION DONE");
  process.exit();
}

migrate();
