import paho.mqtt.client as mqtt

def on_message(client, userdata, message):
    print(f"Received log: {message.payload.decode()}")

client = mqtt.Client()
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.subscribe("logs")

client.loop_forever()