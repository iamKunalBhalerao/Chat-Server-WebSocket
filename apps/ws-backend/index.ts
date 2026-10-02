import { WebSocketServer, type WebSocket } from "ws";

const wss = new WebSocketServer({ port: 8080 });

// users => [[userId, WebSocket]]
const users = new Map<string, WebSocket>();
// rooms => [[roomId, {userId, userId}]]
const rooms = new Map<string, Set<string>>();

wss.on("connection", (socket) => {
  console.log("Connected to WebSocket!");
  let currentUser: string | null = null;

  socket.on("message", (message) => {
    const parsedData = message.toString();

    let parsedJson: any;

    try {
      parsedJson = JSON.parse(parsedData);
    } catch {
      console.log("Received simple string message:", parsedData);
      return;
    }

    if (typeof parsedJson === "object" && parsedJson !== null) {
      // MESSAGE HANDLING
      // IDENTIFY USER HANDLING
      if (parsedJson.type === "identify") {
        const { userId } = parsedJson;
        if (!userId || !userId.trim()) {
          socket.close();
          return;
        }
        users.set(userId, socket);
        currentUser = userId;
        socket.send(
          JSON.stringify({
            type: "user_joined",
            userId,
          }),
        );
      }

      if (parsedJson.type === "direct_message") {
        const { to, message } = parsedJson;

        const reciver = users.get(to);
        if (!reciver) return socket.close();

        reciver.send(
          JSON.stringify({
            type: "recive_message",
            from: currentUser,
            message,
          }),
        );
      }

      //
      //
      //
      //
      //
      //
      //
    } else {
      console.log("Received non-object JSON value:", parsedJson);
      socket.send(
        JSON.stringify({ status: "ignored", message: "dummy response" }),
      );
    }
  });
});
