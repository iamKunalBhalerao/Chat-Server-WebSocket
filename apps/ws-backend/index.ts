import { WebSocketServer, WebSocket } from "ws";

const wss = new WebSocketServer({ port: 8080 });

// users => [[userId, WebSocket]]
const users = new Map<string, WebSocket>();
// rooms => [[roomId, {userId, userId}]]
const rooms = new Map<string, Set<WebSocket>>();

wss.on("connection", (socket) => {
  console.log("Connected to WebSocket!");
  let isAlive: boolean;
  let currentUser: string | null = null;

  isAlive = true;
  socket.on("pong", () => (isAlive = true));

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

      type MessageType =
        | "identify"
        | "direct_message"
        | "join_room"
        | "room_message"
        | "leave_room";

      const handlers = {
        identify: (data: any) => {
          const { userId } = data;
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
        },
        direct_message: (data: any) => {
          const { to, message } = data;

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
              messageId: crypto.randomUUID(),
              from: currentUser,
              message,
            }),
          );
        },
        join_room: (data: any) => {
          const { roomId } = data;
          if (!currentUser) {
            return socket.send(
              JSON.stringify({
                type: "error",
                code: "NOT_IDENTIFIED",
                message: "You are not identified",
              }),
            );
          }
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
        },
        room_message: (data: any) => {
          const { roomId, message } = data;
          const room = rooms.get(roomId);
          if (!room) return;

          if (room!.has(socket)) {
            return socket.send(
              JSON.stringify({
                type: "error",
                code: "NOT_IN_ROOM",
                message: "You are not part of this room",
              }),
            );
          }

          room.forEach((client) => {
            if (client !== socket && client.readyState === WebSocket.OPEN) {
              client.send(
                JSON.stringify({
                  type: "room_message",
                  roomId,
                  from: currentUser,
                  messageId: crypto.randomUUID(),
                  message,
                }),
              );
            }
          });
        },
        leave_room: (data: any) => {
          const { roomId } = data;
          const room = rooms.get(roomId);
          if (room) {
            room.delete(socket);
            if (room.size === 0) {
              rooms.delete(roomId);
            }
          }
        },
      };

      const handler = handlers[parsedJson.type as MessageType];
      if (handler) {
        handler(parsedJson);
      } else {
        socket.send(
          JSON.stringify({
            type: "error",
            code: "UNKNOWN_MESSAGE_TYPE",
            message: "Unknown message type",
          }),
        );
      }
    } else {
      console.log("Received non-object JSON value:", parsedJson);
      socket.send(
        JSON.stringify({ status: "ignored", message: "dummy response" }),
      );
    }
  });

  const interval = setInterval(() => {
    if (isAlive === false) {
      return socket.terminate();
    }
    isAlive = false;
    socket.ping();
  }, 30 * 1000);

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

    clearInterval(interval);
  });
});
