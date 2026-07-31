class_name MqttVibedClientExample
extends Node


signal on_server_to_client_received_byte(package:PackedByteArray)
signal on_server_to_client_received_text(text:String)
#signal on_client_to_server_received_byte(package:PackedByteArray)
#signal on_client_to_server_received_text(text:String)


@export var mqtt_client:MqttVibedClient 
@export var mqtt_url: String = "tcp://test.mosquitto.org:1883"
@export var client_id: String = "Godot Client"
@export var username: String = ""
@export var password: String = ""


##   var mqtt := MqttVibedClient.new()
##   add_child(mqtt)
##   mqtt.received_message.connect(_on_mqtt_message)
##   mqtt.connect_to_broker("tcp://test.mosquitto.org:1883")
##   await mqtt.broker_connected
##   mqtt.subscribe("test/topic", 0)
##   mqtt.publish("test/topic", "hello world")



func _ready():
	mqtt_client.received_message.connect(_on_mqtt_message)
	mqtt_client.connect_to_broker(mqtt_url)
	await mqtt_client.broker_connected
	mqtt_client.subscribe("test/topic", 0)
	mqtt_client.publish("test/topic", "Hello from Godot!")
	
	var timer = Timer.new()
	add_child(timer)
	timer.timeout.connect(_on_timer_timeout)
	timer.start(1.0)


var bytes_screen :PackedByteArray = PackedByteArray()
# random bytes of 128x64 bits (128 * 64 / 8 = 1024 bytes)
func _on_timer_timeout() -> void:
	var random_texts = ["hello", "world", "test", "mqtt", "godot", "random"]
	var random_text = random_texts[randi() % random_texts.size()]
	mqtt_client.publish("test/topic", random_text)
	
	bytes_screen.resize((128*64)/8)
	for i in range(bytes_screen.size()):
		bytes_screen[i] = randi() % 256
	var b64:String = Marshalls.raw_to_base64(bytes_screen)
	var b64_text:String = "b64|" + b64
	print("SIZE OUT: %d" % b64.length() )
	mqtt_client.publish("test/topic", b64_text)
	

func _on_mqtt_message(topic:String, message) -> void:
	var payload = message.payload.get_string_from_utf8() if message is Object and "payload" in message else str(message)
	print("Received message on topic '%s': %s" % [topic, payload])
	if payload.begins_with("b64|"):
		var b64_data = payload.substr(4, payload.length() - 4)
		var byte_array = Marshalls.base64_to_raw(b64_data)
		print ("SIZE IN: %d" % byte_array.size())
		on_server_to_client_received_byte.emit(byte_array)
	else:
		on_server_to_client_received_text.emit(payload)
	

	
