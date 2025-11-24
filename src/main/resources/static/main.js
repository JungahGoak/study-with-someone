// ========================================
// 전역 변수
// ========================================
let ws = null;
let pc = null;

let myId = null;
let targetId = null;

let localStream = null;

const rtcConfig = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" }
    ]
};

// ========================================
// WebSocket 연결
// ========================================
function connectToServer() {
    ws = new WebSocket("wss://localhost:8080/signal");

    ws.onopen = () => {
        addLog("🟢 WebSocket connected");
    };

    ws.onmessage = async (event) => {
        const msg = JSON.parse(event.data);
        handleSignal(msg);
    };

    ws.onclose = () => {
        addLog("🔴 WebSocket closed");
        cleanupConnection();
    };
}

function disconnectFromServer() {
    if (ws) ws.close();
    cleanupConnection();
}

// ========================================
// 시그널 메시지 처리
// ========================================
async function handleSignal(msg) {
    const { type, myId: from, targetId: to } = msg;

    // 최초 연결 시 서버가 내 id 알려주지 않음 → sessionId 사용
    // but backend does NOT send id → we only know myId when sending/receiving
    if (!myId) myId = from;
    document.getElementById("myPeerId").innerText = myId;

    switch (type) {
        // ========================================
        // 🔵 MATCH - Publisher / Subscriber 결정
        // ========================================
        case "PUBLISH":
            addLog(`📤 역할: PUBLISHER (상대=${to})`);
            targetId = to;
            startAsPublisher();
            break;

        case "SUBSCRIBE":
            addLog(`📥 역할: SUBSCRIBER (상대=${to})`);
            targetId = to;
            startAsSubscriber();
            break;

        // ========================================
        // WebRTC signaling
        // ========================================
        case "OFFER":
            await handleOffer(msg);
            break;

        case "ANSWER":
            await handleAnswer(msg);
            break;

        case "ICE":
            await handleIceCandidate(msg);
            break;

        case "LEAVE":
            addLog("🔴 상대방 연결 종료");
            cleanupConnection();
            break;
    }
}

// ========================================
// Publisher 역할 - Offer 생성
// ========================================
async function startAsPublisher() {
    pc = createPeerConnection();

    // local media
    localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
    document.getElementById("localVideo").srcObject = localStream;

    // Add tracks
    localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

    // Create Offer
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    sendSignal({
        type: "OFFER",
        targetId,
        sdp: offer
    });
}

// ========================================
// Subscriber 역할 - Offer 수신 → Answer 생성
// ========================================
async function startAsSubscriber() {
    pc = createPeerConnection();

    localStream = await navigator.mediaDevices.getUserMedia({ video: false, audio: false });
    document.getElementById("localVideo").srcObject = null;
}

// ========================================
// WebRTC Offer 처리
// ========================================
async function handleOffer(msg) {
    if (!pc) pc = createPeerConnection();

    await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    sendSignal({
        type: "ANSWER",
        targetId: msg.myId,
        sdp: answer
    });
}

// ========================================
// WebRTC Answer 처리
// ========================================
async function handleAnswer(msg) {
    if (!pc) return;
    await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
}

// ========================================
// ICE 후보 처리
// ========================================
async function handleIceCandidate(msg) {
    if (msg.candidate && pc) {
        await pc.addIceCandidate(new RTCIceCandidate(msg.candidate));
    }
}

// ========================================
// PeerConnection 생성
// ========================================
function createPeerConnection() {
    const pc = new RTCPeerConnection(rtcConfig);

    // remote video
    pc.ontrack = (event) => {
        document.getElementById("remoteVideo").srcObject = event.streams[0];
    };

    // ICE candidate
    pc.onicecandidate = (event) => {
        if (event.candidate) {
            sendSignal({
                type: "ICE",
                targetId,
                candidate: event.candidate
            });
        }
    };

    // connection state
    pc.onconnectionstatechange = () => {
        addLog(`🔗 PC state: ${pc.connectionState}`);
    };

    return pc;
}

// ========================================
// WebSocket으로 signaling 메시지 전송
// ========================================
function sendSignal(msg) {
    ws.send(JSON.stringify(msg));
}

// ========================================
// 연결 및 UI 초기화
// ========================================
function cleanupConnection() {
    if (pc) {
        pc.close();
        pc = null;
    }
    targetId = null;
    document.getElementById("remoteVideo").srcObject = null;
    addLog("🔄 Reset local state");
}

// ========================================
// 로그 출력
// ========================================
function addLog(msg) {
    const logBox = document.getElementById("logBox");
    logBox.innerHTML += msg + "\n";
    logBox.scrollTop = logBox.scrollHeight;
}
