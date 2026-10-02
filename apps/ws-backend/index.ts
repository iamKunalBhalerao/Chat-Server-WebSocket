import { WebSocketServer, WebSocket } from "ws";

const wss = new WebSocketServer({ port: 8080 });

// users => [[userId, WebSocket]]
const users = new Map<string, WebSocket>();
// rooms => [[roomId, {userId, userId}]]
const rooms = new Map<string, Set<WebSocket>>();

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
          return socket.send(
            JSON.stringify({
              type: "error",
              code: "INVALID_USERID",
              message: "Invalid UserId",
            }),
          );
        }
        if (currentUser) {
          return socket.send(
            JSON.stringify({
              type: "error",
              code: "ALREADY_IDENTIFIED",
              message: "You have already identified",
            }),
          );
        } else if (users.has(userId)) {
          return socket.send(
            JSON.stringify({
              type: "error",
              code: "USER_ALREADY_EXISTS",
              message: "You already identified!",
            }),
          );
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

        if (!currentUser) {
          return socket.send(
            JSON.stringify({
              type: "error",
              code: "NOT_IDENTIFIED",
              message: "You are not identified",
            }),
          );
        }

        const reciver = users.get(to);
        if (!reciver) {
          socket.send(
            JSON.stringify({
              type: "error",
              code: "USER_NOT_FOUND",
              message: "User is not online",
            }),
          );
          return;
        }

        reciver.send(
          JSON.stringify({
            type: "recive_message",
            from: currentUser,
            message,
          }),
        );
      }

      if (parsedJson.type === "join_room") {
        const { roomId } = parsedJson;
        if (!rooms.has(roomId)) {
          rooms.set(roomId, new Set([socket]));
        } else if (rooms.get(roomId)!.has(socket)) {
          return socket.send(
            JSON.stringify({
              type: "error",
              code: "ALREADY_IN_ROOM",
              message: "You are already in the room",
            }),
          );
        } else {
          rooms.get(roomId)!.add(socket);
        }
      }

      if (parsedJson.type === "room_message") {
        const { roomId, message } = parsedJson;
        const room = rooms.get(roomId);
        if (!room) return;

        room.forEach((client) => {
          if (client !== socket && client.readyState === WebSocket.OPEN) {
            client.send(
              JSON.stringify({
                type: "room_message",
                roomId,
                from: currentUser,
                message,
              }),
            );
          }
        });
      }

      if (parsedJson.type === "leave_room") {
        const { roomId } = parsedJson;
        const room = rooms.get(roomId);
        if (room) {
          room.delete(socket);
          if (room.size === 0) {
            rooms.delete(roomId);
          }
        }
      }
    } else {
      console.log("Received non-object JSON value:", parsedJson);
      socket.send(
        JSON.stringify({ status: "ignored", message: "dummy response" }),
      );
    }
  });

  socket.on("close", () => {
    if (currentUser) {
      users.delete(currentUser);
    }
    rooms.forEach((clients, roomId) => {
      clients.delete(socket);
      if (clients.size === 0) {
        rooms.delete(roomId);
      }
    });
  });
});
