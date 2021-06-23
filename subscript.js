var mqtt = require("mqtt");

var client = mqtt.connect("mqtt://broker.mqttdashboard.com");

client.on("message", function (topic, message) {
  console.log(message.toString());
});
client.on("connect", function (topic, message) {
  client.subscribe("basket/1");
});
