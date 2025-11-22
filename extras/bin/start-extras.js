// extras/bin/start-extras.js
import { main as twitterProfiles } from "../jobs/twitter-profiles.js";

const EVERY_MINUTES = Number(process.env.EXTRAS_POLL_MIN || 60);

async function tick() {
  try {
    await twitterProfiles();
  } catch (e) {
    console.error("twitter-profiles error:", e);
  }
}

(async () => {
  console.log(`[extras] starting; poll every ${EVERY_MINUTES} min`);
  await tick(); // run once at boot
  setInterval(tick, EVERY_MINUTES * 60 * 1000);
})();
