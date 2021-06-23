require("dotenv").config();
const express = require("express");
var bodyParser = require("body-parser");
const app = express();
const http = require("http");
const server = http.createServer(app);
const mqtt = require("mqtt");
const { Server } = require("socket.io");
const { Pool } = require("pg");
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});
const connectionString = process.env.DATABASE_URI;
var pool = new Pool({ connectionString });
const mqttClient = mqtt.connect(process.env.MQTT_BROKER);

app.use(bodyParser.json());

mqttClient.on("connect", function () {
  mqttClient.subscribe("baskets/#");
  mqttClient.subscribe("store_and_send");
});

mqttClient.on("message", function (topic, message) {
  // message is Buffer
  const payload = JSON.parse(message.toString());
  if (topic === "store_and_send") {
    if (
      !("bakset_id" in payload) ||
      !("category" in payload) ||
      !("waste_height" in payload)
    )
      return;
    pool.connect().then(async (client) => {
      let current_section_level = 0;
      try {
        await client.query("BEGIN");
        const basket = await client.query(
          "SELECT fullness_level, length, width from basket_section where basket_id = $1 AND category = $2 ;",
          [payload.basket_id, payload.category]
        );
        const { fullness_level, length, width } = basket.rows[0];
        current_section_level = fullness_level + payload.waste_height;

        await client.query(
          "UPDATE basket_section set fullness_level = $1 WHERE  category = $2 AND basket_id = $3 ;",
          [
            fullness_level + payload.waste_height,
            payload.category,
            payload.basket_id,
          ]
        );
        await client.query(
          "INSERT INTO wastes(height, size, basket_id, category) VALUES ($1, $2, $3, $4);",
          [
            payload.waste_height,
            payload.waste_height * width * length,
            payload.basket_id,
            payload.category,
          ]
        );
        await client.query("COMMIT");
        client.release();
        if (current_section_level > 90) {
          io.emit("notification", {
            title: "basket #" + payload.basket_id,
            primary:
              payload.category + " Section ---  " + current_section_level + "%",
            secondary: "and the total basket level is 70% please take action",
          });
        }
      } catch (err_1) {
        client.release();
        console.log(err_1.stack);
      }
    });

    payload.date = new Date().getTime() / 1000;
    io.emit("add-wastes", { wastes: payload });
  }
  if (topic === "update_status") console.log(message.toString());
});

app.post("/:topic/:sub_topic", (req, res) => {
  const data = JSON.stringify(req.body);
  const topic = req.params.topic + "/" + req.params.sub_topic;
  client.publish(topic, data, (error) => {
    if (!error) res.end(JSON.stringify({ success: true }));
    else res.end(JSON.stringify({ success: false }));
  });
});

io.on("connection", (socket) => {
  console.log("a user connected");
  socket.broadcast.emit("connection", "you are connnected");
  io.emit("connection", "connected");
});

server.listen(process.env.PORT || 4000, () => {
  console.log(`listening on *:${process.env.PORT || 4000}`);
});
