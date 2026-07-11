class_name MqttVibedClientExample
extends Node


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


func _on_timer_timeout() -> void:
	var random_texts = ["hello", "world", "test", "mqtt", "godot", "random"]
	var random_text = random_texts[randi() % random_texts.size()]
	mqtt_client.publish("test/topic", random_text)


func _on_mqtt_message(topic:String, message) -> void:
	var payload = message.payload.get_string_from_utf8() if message is Object and "payload" in message else str(message)
	print("Received message on topic '%s': %s" % [topic, payload])

	
