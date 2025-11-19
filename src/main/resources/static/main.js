// main.js — Final WebRTC Client (Publisher = no remote video)

const WS_URL = "ws://localhost:8080/signal";

let ws;
let pc = null;

let myId = null;
let targetId = null;
let role = null;   // "PUBLISHER" or "SUBSCRIBER"

let localStream = null;
const rtcConfig = {
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
};

const logBox = document.getElementById("logs");
const localVideo = document.getElementById("localVideo");
const remoteVideo = document.getElementById("remoteVideo");

function log(...args) {
    const s = args
        .map(a => (typeof a === "object" ? JSON.stringify(a) : String(a)))
        .join(" ");
    console.log(...args);
    logBox.textContent += s + "\n";
    logBox.scrollTop = logBox.scrollHeight;
}

// ---------------------------------------
// Buffers
// ---------------------------------------
const pendingOfferMap = new Map(); // publisherId -> msg
const pendingIceMap = new Map();   // peerId -> [candidate...]

function bufferIce(peerId, candidate) {
    if (!pendingIceMap.has(peerId)) pendingIceMap.set(peerId, []);
    pendingIceMap.get(peerId).push(candidate);
}

async function flushIce(peerId) {
    const arr = pendingIceMap.get(peerId);
    if (!arr || !pc) return;

    for (const cand of arr) {
        try {
            await pc.addIceCandidate(cand);
            log("Flushed ICE for", peerId);
        } catch (e) {
            log("Failed to flush ICE:", e);
        }
    }
    pendingIceMap.delete(peerId);
}

// ---------------------------------------
// Media
// ---------------------------------------
async function startCamera() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({
            video: true,
            audio: false
        });

        localVideo.srcObject = localStream;
        log("Local camera started");

        connectWebSocket();
    } catch (e) {
        log("Camera error:", e);
        alert("Camera start failed: " + e.message);
    }
}

document.getElementById("startBtn").onclick = startCamera;

// ---------------------------------------
// WebSocket
// ---------------------------------------
function connectWebSocket() {
    if (ws && ws.readyState === WebSocket.OPEN) return;

    ws = new WebSocket(WS_URL);

    ws.onopen = () => log("🌐 WebSocket connected");

    ws.onmessage = async (ev) => {
        let msg;
        try {
            msg = JSON.parse(ev.data);
        } catch (_) {
            log("WS parse error:", ev.data);
            return;
        }

        log("📩 RECV:", msg);

        myId = msg.myId ?? myId;

        switch (msg.type) {
            case "PUBLISH":   await onPublish(msg); break;
            case "SUBSCRIBE": await onSubscribe(msg); break;
            case "OFFER":     await onOffer(msg); break;
            case "ANSWER":    await onAnswer(msg); break;
            case "ICE":       await onIce(msg); break;
            case "LEAVE":     onLeave(msg); break;
            default:
                log("Unknown type:", msg.type);
        }
    };

    ws.onclose = (e) => log("🔌 WS closed", e.code, e.reason);
    ws.onerror = (e) => log("❌ WS error", e);
}

function sendSignal(obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        log("WS not open, cannot send:", obj);
        return;
    }
    obj.myId = myId;
    ws.send(JSON.stringify(obj));
    log("→ WS SENT:", obj.type, obj.targetId ?? "");
}

// ---------------------------------------
// PeerConnection
// ---------------------------------------
function ensurePC() {
    if (pc) return pc;

    pc = new RTCPeerConnection(rtcConfig);

    pc.onicecandidate = (e) => {
        if (!e.candidate) return;
        if (!targetId) return log("ICE without targetId → drop");

        sendSignal({
            type: "ICE",
            targetId,
            data: JSON.stringify({ candidate: e.candidate })
        });
    };

    pc.ontrack = (e) => {
        log("ontrack event");

        // 🔥 Publisher는 remoteVideo를 갱신하면 안됨
        if (role === "PUBLISHER") {
            log("PUBLISHER → ignoring remote track");
            return;
        }

        // Subscriber만 remoteVideo 표시
        if (e.streams && e.streams[0]) {
            remoteVideo.srcObject = e.streams[0];
        } else {
            const stream = new MediaStream();
            stream.addTrack(e.track);
            remoteVideo.srcObject = stream;
        }
    };

    pc.onconnectionstatechange = () => {
        log("PC state:", pc.connectionState);
    };

    // Publisher일 때만 local tracks 추가
    if (localStream) {
        localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
    }

    log("RTCPeerConnection created");
    return pc;
}

// ---------------------------------------
// Parse utils
// ---------------------------------------
function parseData(msg) {
    if (msg.data) {
        try {
            return typeof msg.data === "string" ? JSON.parse(msg.data) : msg.data;
        } catch {
            return null;
        }
    }
    if (msg.sdp) return msg.sdp;
    if (msg.candidate) return { candidate: msg.candidate };
    return null;
}

// ---------------------------------------
// Handlers: PUBLISH / SUBSCRIBE / OFFER / ANSWER / ICE / LEAVE
// ---------------------------------------

async function onPublish(msg) {
    // role = publisher, create offer and send
    role = "PUBLISHER";
    targetId = msg.targetId;
    log("Role -> PUBLISHER target:", targetId);

    // ensure pc and add local tracks (if not already added)
    ensurePC();

    // If localStream exists and tracks may not have been added into this pc (e.g. pc created earlier),
    // add them now. We guard by checking senders length.
    try {
        if (localStream && pc.getSenders().length === 0) {
            localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
            log("Added local tracks to PC (publisher)");
        }
    } catch (e) {
        log("Error adding local tracks (publisher):", e);
    }

    try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);

        sendSignal({
            type: "OFFER",
            targetId,
            data: JSON.stringify({ sdp: offer.sdp, type: offer.type })
        });
        log("📤 Sent OFFER to", targetId);
    } catch (e) {
        log("Failed create/send OFFER:", e);
    }
}

async function onSubscribe(msg) {
    // role = subscriber, wait for offer
    role = "SUBSCRIBER";
    targetId = msg.targetId;
    log("Role -> SUBSCRIBER publisher:", targetId);

    // Create pc; do NOT add local tracks by default (subscriber is receive-only)
    ensurePC();
    // If localStream exists but user is subscriber, we do NOT add track automatically.
    // (If you want subscriber to also publish audio/video, change policy here.)
    log("Subscriber ready; awaiting OFFER");
}

async function onOffer(msg) {
    // Offer may have data=null; parse and buffer if needed
    const payload = parseData(msg);

    if (!payload || !payload.sdp) {
        // Buffer the offer (keyed by publisher id)
        log("⚠ OFFER missing SDP — buffering. from:", msg.myId);
        pendingOfferMap.set(msg.myId, msg);
        return;
    }

    try {
        // Ensure pc exists and role is subscriber (defensive)
        if (!pc) ensurePC();
        if (!role) role = "SUBSCRIBER"; // defensive: if no explicit role, become subscriber

        await pc.setRemoteDescription({ type: payload.type || "offer", sdp: payload.sdp });
        log("Remote description (offer) set from", msg.myId);

        // flush any buffered ICE for this peer
        await flushIce(msg.myId);

        // Create and send answer
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);

        sendSignal({
            type: "ANSWER",
            targetId: msg.myId,
            data: JSON.stringify({ sdp: answer.sdp, type: answer.type })
        });
        log("📤 Sent ANSWER to", msg.myId);
    } catch (e) {
        log("Error handling OFFER:", e);
    }
}

async function onAnswer(msg) {
    const payload = parseData(msg);
    if (!payload || !payload.sdp) {
        log("⚠ ANSWER missing SDP — ignoring");
        return;
    }

    try {
        if (!pc) {
            log("No pc when receiving ANSWER — creating");
            ensurePC();
        }
        await pc.setRemoteDescription({ type: payload.type || "answer", sdp: payload.sdp });
        log("Remote description (answer) applied");

        // flush buffered ICEs for this peer (if any)
        await flushIce(msg.myId);
    } catch (e) {
        log("Error applying ANSWER:", e);
    }
}

async function onIce(msg) {
    const payload = parseData(msg);
    if (!payload || !payload.candidate) {
        log("⚠ ICE missing candidate");
        return;
    }

    try {
        // If pc.remoteDescription exists, add immediately
        if (pc && pc.remoteDescription && pc.remoteDescription.type) {
            await pc.addIceCandidate(payload.candidate);
            log("📩 ICE candidate added (immediate)");
        } else {
            // buffer per-sender
            log("Buffering ICE candidate until remoteDescription present for", msg.myId);
            bufferIce(msg.myId, payload.candidate);
        }
    } catch (e) {
        log("Error adding ICE candidate:", e);
    }
}

function onLeave(msg) {
    log("🔴 Received LEAVE from", msg.myId);
    // If the other peer left, cleanup this connection (but keep ws open)
    cleanup();
}

// ---------------------------------------
// Cleanup
// ---------------------------------------
function cleanup() {
    try {
        if (pc) pc.close();
    } catch (e) { /* ignore */ }
    pc = null;
    remoteVideo.srcObject = null;
    pendingOfferMap.clear();
    pendingIceMap.clear();
    log("Cleaned up peer connection and buffers");
}

// ---------------------------------------
// Pending ICE helper
// ---------------------------------------
function bufferIce(peerId, candidate) {
    if (!pendingIceMap.has(peerId)) pendingIceMap.set(peerId, []);
    pendingIceMap.get(peerId).push(candidate);
}

async function flushIce(peerId) {
    const arr = pendingIceMap.get(peerId);
    if (!arr || !pc) return;
    for (const cand of arr) {
        try {
            await pc.addIceCandidate(cand);
            log("Flushed buffered ICE for", peerId);
        } catch (e) {
            log("Failed to add buffered ICE:", e);
        }
    }
    pendingIceMap.delete(peerId);
}

// ---------------------------------------
// Periodically try to apply buffered offers
// ---------------------------------------
async function tryApplyPendingOffers() {
    for (const [fromId, msg] of pendingOfferMap.entries()) {
        const payload = parseData(msg);
        if (!payload || !payload.sdp) continue;
        log("Applying previously buffered OFFER from", fromId);
        pendingOfferMap.delete(fromId);

        // reuse onOffer to handle it
        await onOffer(msg);
    }
}
setInterval(() => {
    tryApplyPendingOffers().catch(e => log("tryApplyPendingOffers err", e));
}, 1500);

// ---------------------------------------
// Before unload: notify server and cleanup
// ---------------------------------------
window.addEventListener("beforeunload", () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        try {
            sendSignal({ type: "LEAVE", targetId });
        } catch (e) { /* ignore */ }
    }
    cleanup();
});
