class_name MqttVibedClient
extends Node

## A small MQTT v3.1.1 client for Godot 4.
##
## Supported broker URLs:
##   tcp://host:port         - plain TCP            (default port 1883)
##   ssl://host:port         - TLS over TCP         (default port 8883)
##   ws://host:port/path     - WebSocket            (default port 8080)
##   wss://host:port/path    - Secure WebSocket     (default port 8081)
##   host:port               - defaults to tcp://
##
## Example:
##   var mqtt := MqttVibedClient.new()
##   add_child(mqtt)
##   mqtt.received_message.connect(_on_mqtt_message)
##   mqtt.connect_to_broker("tcp://test.mosquitto.org:1883")
##   await mqtt.broker_connected
##   mqtt.subscribe("test/topic", 0)
##   mqtt.publish("test/topic", "hello world")

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

@export var client_id: String = ""
@export_range(0, 2) var verbose_level: int = 2
@export var binary_messages: bool = false
@export_range(5.0, 600.0) var ping_interval: float = 30.0
@export var keep_alive: int = 120

# -----------------------------------------------------------------------------
# Signals
# -----------------------------------------------------------------------------

signal received_message(topic: String, message)          # message: String or PackedByteArray
signal broker_connected
signal broker_disconnected
signal broker_connection_failed
signal publish_acknowledged(packet_id: int)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

# MQTT 3.1.1 control packet types (high nibble; low nibble holds flags).
const CP_CONNECT     := 0x10
const CP_CONNACK     := 0x20
const CP_PUBLISH     := 0x30
const CP_PUBACK      := 0x40
const CP_SUBSCRIBE   := 0x82
const CP_SUBACK      := 0x90
const CP_UNSUBSCRIBE := 0xA2
const CP_UNSUBACK    := 0xB0
const CP_PINGREQ     := 0xC0
const CP_PINGRESP    := 0xD0
const CP_DISCONNECT  := 0xE0

const MQTT_PROTOCOL_LEVEL := 0x04  # MQTT v3.1.1
const MQTT_MAGIC := "MQTT"

const DEFAULT_PORT_TCP := 1883
const DEFAULT_PORT_SSL := 8883
const DEFAULT_PORT_WS  := 8080
const DEFAULT_PORT_WSS := 8081

# -----------------------------------------------------------------------------
# Internal state
# -----------------------------------------------------------------------------

enum State {
	DISCONNECTED,
	CONNECTING_WEBSOCKET,
	CONNECTING_TCP,
	CONNECTING_SSL,
	WAITING_FOR_CONNACK,
	CONNECTED,
}

var _state: State = State.DISCONNECTED

# Only one of these is non-null at a time.
var _tcp: StreamPeerTCP = null
var _tls: StreamPeerTLS = null
var _ws: WebSocketPeer = null
var _tls_hostname: String = ""

var _rx_buffer := PackedByteArray()
var _packet_id := 0
var _next_ping_ms := 0

# Credentials
var _username := PackedByteArray()
var _password := PackedByteArray()
var _has_credentials := false

# Last Will & Testament
var _will_topic := PackedByteArray()
var _will_message := PackedByteArray()
var _will_qos := 0
var _will_retain := false
var _has_will := false

var _url_regex := RegEx.new()

# =============================================================================
# Lifecycle
# =============================================================================

func _ready() -> void:
	_url_regex.compile(r'^(tcp://|ssl://|ws://|wss://)?([^:\s]+)(:\d+)?(/\S*)?$')
	if client_id.is_empty():
		client_id = "godot-%d" % randi()


func _process(_delta: float) -> void:
	match _state:
		State.DISCONNECTED:
			pass
		State.CONNECTING_WEBSOCKET:
			_tick_ws_connecting()
		State.CONNECTING_TCP:
			_tick_tcp_connecting()
		State.CONNECTING_SSL:
			_tick_ssl_connecting()
		State.WAITING_FOR_CONNACK, State.CONNECTED:
			_tick_active()


func _tick_active() -> void:
	_read_into_buffer()
	while _process_one_packet():
		pass
	if _state == State.CONNECTED and Time.get_ticks_msec() >= _next_ping_ms:
		_send_pingreq()
		_next_ping_ms = Time.get_ticks_msec() + int(ping_interval * 1000.0)

# =============================================================================
# Connection state-machine handlers
# =============================================================================

func _tick_ws_connecting() -> void:
	_ws.poll()
	match _ws.get_ready_state():
		WebSocketPeer.STATE_CLOSED:
			_log(1, "WebSocket closed (code=%d, reason=%s)" % [_ws.get_close_code(), _ws.get_close_reason()])
			_enter_failed()
		WebSocketPeer.STATE_OPEN:
			_log(1, "WebSocket connection open")
			_begin_mqtt_handshake()


func _tick_tcp_connecting() -> void:
	_tcp.poll()
	match _tcp.get_status():
		StreamPeerTCP.STATUS_ERROR:
			_log(1, "TCP socket error")
			_enter_failed()
		StreamPeerTCP.STATUS_CONNECTED:
			_log(1, "TCP connected")
			_begin_mqtt_handshake()


func _tick_ssl_connecting() -> void:
	_tcp.poll()
	var s := _tcp.get_status()
	if s == StreamPeerTCP.STATUS_ERROR:
		_log(1, "TCP socket error before SSL handshake")
		_enter_failed()
		return
	if s != StreamPeerTCP.STATUS_CONNECTED:
		return

	if _tls == null:
		_tls = StreamPeerTLS.new()
		_log(1, "Starting TLS handshake with hostname=%s" % _tls_hostname)
		var err := _tls.connect_to_stream(_tcp, _tls_hostname)
		if err != OK:
			_log(0, "TLS connect_to_stream failed: %d" % err)
			_tls = null
			_enter_failed()
			return

	_tls.poll()
	match _tls.get_status():
		StreamPeerTLS.STATUS_CONNECTED:
			_log(1, "TLS handshake complete")
			_begin_mqtt_handshake()
		StreamPeerTLS.STATUS_ERROR, StreamPeerTLS.STATUS_ERROR_HOSTNAME_MISMATCH:
			_log(0, "TLS handshake failed (status=%d)" % _tls.get_status())
			_enter_failed()


func _begin_mqtt_handshake() -> void:
	_send_data(_build_connect_packet())
	_state = State.WAITING_FOR_CONNACK


func _enter_failed() -> void:
	broker_connection_failed.emit()
	_cleanup()

# =============================================================================
# Public API
# =============================================================================

func is_connected_to_broker() -> bool:
	return _state == State.CONNECTED


func set_user_pass(username: String, password: String) -> void:
	if username.is_empty():
		_has_credentials = false
		_username.clear()
		_password.clear()
	else:
		_has_credentials = true
		_username = username.to_utf8_buffer()
		_password = password.to_utf8_buffer()


func set_last_will(topic: String, message, retain: bool = false, qos: int = 0) -> void:
	assert(qos >= 0 and qos <= 2, "QoS must be 0, 1, or 2")
	assert(not topic.is_empty(), "Last-will topic must not be empty")
	_has_will = true
	_will_topic = topic.to_utf8_buffer()
	_will_message = _coerce_payload(message)
	_will_qos = qos
	_will_retain = retain
	_log(1, "Last-will set: topic=%s retain=%s qos=%d" % [topic, retain, qos])


func connect_to_broker(broker_url: String) -> bool:
	assert(_state == State.DISCONNECTED, "MQTT client is already in use")

	var m = _url_regex.search(broker_url)
	if m == null:
		push_error("MQTT: unrecognized broker URL: %s" % broker_url)
		return false

	var scheme: String = m.strings[1]
	var host: String   = m.strings[2]
	var port_s: String = m.strings[3]
	var path: String   = m.strings[4]

	var is_ws  := scheme == "ws://" or scheme == "wss://"
	var is_tls := scheme == "ssl://" or scheme == "wss://"

	var port: int
	if not port_s.is_empty():
		port = int(port_s.substr(1))
	elif is_ws:
		port = DEFAULT_PORT_WSS if is_tls else DEFAULT_PORT_WS
	else:
		port = DEFAULT_PORT_SSL if is_tls else DEFAULT_PORT_TCP

	_tls_hostname = host

	if is_ws:
		var ws_url := ("wss://" if is_tls else "ws://") + host + ":" + str(port) + path
		_log(1, "Connecting to %s" % ws_url)
		_ws = WebSocketPeer.new()
		_ws.supported_protocols = PackedStringArray(["mqttv3.1"])
		var err := _ws.connect_to_url(ws_url)
		if err != OK:
			push_error("MQTT: WebSocket connect_to_url failed: %d" % err)
			_ws = null
			return false
		_state = State.CONNECTING_WEBSOCKET
	else:
		_log(1, "Connecting to %s:%d" % [host, port])
		_tcp = StreamPeerTCP.new()
		var err := _tcp.connect_to_host(host, port)
		if err != OK:
			push_error("MQTT: TCP connect_to_host failed: %d" % err)
			_tcp = null
			return false
		_state = State.CONNECTING_SSL if is_tls else State.CONNECTING_TCP

	return true


func disconnect_from_server() -> void:
	if _state == State.CONNECTED:
		_send_data(PackedByteArray([CP_DISCONNECT, 0x00]))
		broker_disconnected.emit()
	_cleanup()


func publish(topic: String, message, retain: bool = false, qos: int = 0) -> int:
	assert(qos >= 0 and qos <= 2, "QoS must be 0, 1, or 2")
	var pkt := _build_publish_packet(topic, message, retain, qos)
	_send_data(pkt)
	var sent_pid := _packet_id if qos > 0 else 0
	_log(2, "PUBLISH%s%s topic=%s (%d bytes)" % [
		"[%d]" % sent_pid if qos else "",
		" <retain>" if retain else "",
		topic,
		pkt.size(),
	])
	return sent_pid


func subscribe(topic: String, qos: int = 0) -> int:
	assert(qos >= 0 and qos <= 2, "QoS must be 0, 1, or 2")
	_next_packet_id()
	_send_data(_build_subscribe_packet(topic, qos))
	_log(1, "SUBSCRIBE[%d] topic=%s qos=%d" % [_packet_id, topic, qos])
	return _packet_id


func unsubscribe(topic: String) -> int:
	_next_packet_id()
	_send_data(_build_unsubscribe_packet(topic))
	_log(1, "UNSUBSCRIBE[%d] topic=%s" % [_packet_id, topic])
	return _packet_id

# =============================================================================
# Transport I/O
# =============================================================================

func _send_data(data: PackedByteArray) -> Error:
	var err: Error = FAILED
	if _tls != null:
		err = _tls.put_data(data)
	elif _tcp != null:
		err = _tcp.put_data(data)
	elif _ws != null:
		err = _ws.put_packet(data)
	if err != OK:
		push_error("MQTT: send_data failed (err=%d)" % err)
	return err


func _read_into_buffer() -> void:
	if _tls != null:
		var status := _tls.get_status()
		if status != StreamPeerTLS.STATUS_CONNECTED and status != StreamPeerTLS.STATUS_HANDSHAKING:
			return
		_tls.poll()
		var n := _tls.get_available_bytes()
		if n > 0:
			var dv := _tls.get_data(n)
			if dv[0] == OK:
				_rx_buffer.append_array(dv[1])
	elif _tcp != null:
		if _tcp.get_status() != StreamPeerTCP.STATUS_CONNECTED:
			return
		_tcp.poll()
		var n := _tcp.get_available_bytes()
		if n > 0:
			var dv := _tcp.get_data(n)
			if dv[0] == OK:
				_rx_buffer.append_array(dv[1])
	elif _ws != null:
		_ws.poll()
		while _ws.get_available_packet_count() > 0:
			_rx_buffer.append_array(_ws.get_packet())


func _cleanup() -> void:
	_log(1, "Cleaning up sockets")
	_tls = null
	if _tcp != null:
		_tcp.disconnect_from_host()
		_tcp = null
	if _ws != null:
		_ws.close()
		_ws = null
	_rx_buffer.clear()
	_state = State.DISCONNECTED


func _next_packet_id() -> void:
	_packet_id = (_packet_id + 1) & 0xFFFF
	if _packet_id == 0:
		_packet_id = 1

# =============================================================================
# Outgoing packet builders
# =============================================================================

func _build_connect_packet() -> PackedByteArray:
	var client_id_bytes := client_id.to_utf8_buffer()

	var payload_size := 10 + 2 + client_id_bytes.size()
	if _has_will:
		payload_size += 2 + _will_topic.size() + 2 + _will_message.size()
	if _has_credentials:
		payload_size += 2 + _username.size() + 2 + _password.size()

	var pkt := PackedByteArray()
	pkt.append(CP_CONNECT)
	_encode_remaining_length(pkt, payload_size)

	var before_payload := pkt.size()
	_encode_string(pkt, MQTT_MAGIC.to_utf8_buffer())
	pkt.append(MQTT_PROTOCOL_LEVEL)

	var flags := 0x02  # clean session
	if _has_will:
		flags |= 0x04
		flags |= (_will_qos & 0x03) << 3
		if _will_retain:
			flags |= 0x20
	if _has_credentials:
		flags |= 0x80  # username flag
		flags |= 0x40  # password flag
	pkt.append(flags)

	_encode_short(pkt, keep_alive)
	_encode_string(pkt, client_id_bytes)
	if _has_will:
		_encode_string(pkt, _will_topic)
		_encode_string(pkt, _will_message)
	if _has_credentials:
		_encode_string(pkt, _username)
		_encode_string(pkt, _password)

	assert(pkt.size() - before_payload == payload_size, "CONNECT payload size mismatch")
	return pkt


func _build_publish_packet(topic: String, message, retain: bool, qos: int) -> PackedByteArray:
	var topic_bytes := topic.to_utf8_buffer()
	var msg_bytes   := _coerce_payload(message)
	var pkt_id := 0
	if qos > 0:
		_next_packet_id()
		pkt_id = _packet_id

	var payload_size := 2 + topic_bytes.size() + msg_bytes.size()
	if qos > 0:
		payload_size += 2

	var pkt := PackedByteArray()
	var header := CP_PUBLISH | ((qos & 0x03) << 1)
	if retain:
		header |= 0x01
	pkt.append(header)
	_encode_remaining_length(pkt, payload_size)

	var before_payload := pkt.size()
	_encode_string(pkt, topic_bytes)
	if qos > 0:
		_encode_short(pkt, pkt_id)
	pkt.append_array(msg_bytes)
	assert(pkt.size() - before_payload == payload_size, "PUBLISH payload size mismatch")
	return pkt


func _build_subscribe_packet(topic: String, qos: int) -> PackedByteArray:
	var topic_bytes  := topic.to_utf8_buffer()
	var payload_size := 2 + 2 + topic_bytes.size() + 1
	var pkt := PackedByteArray()
	pkt.append(CP_SUBSCRIBE)
	_encode_remaining_length(pkt, payload_size)
	var before_payload := pkt.size()
	_encode_short(pkt, _packet_id)
	_encode_string(pkt, topic_bytes)
	pkt.append(qos & 0x03)
	assert(pkt.size() - before_payload == payload_size, "SUBSCRIBE payload size mismatch")
	return pkt


func _build_unsubscribe_packet(topic: String) -> PackedByteArray:
	var topic_bytes  := topic.to_utf8_buffer()
	var payload_size := 2 + 2 + topic_bytes.size()
	var pkt := PackedByteArray()
	pkt.append(CP_UNSUBSCRIBE)
	_encode_remaining_length(pkt, payload_size)
	var before_payload := pkt.size()
	_encode_short(pkt, _packet_id)
	_encode_string(pkt, topic_bytes)
	assert(pkt.size() - before_payload == payload_size, "UNSUBSCRIBE payload size mismatch")
	return pkt


func _send_pingreq() -> void:
	_log(2, "PINGREQ")
	_send_data(PackedByteArray([CP_PINGREQ, 0x00]))

# =============================================================================
# Incoming packet processing
# =============================================================================

# Parses one MQTT packet from _rx_buffer. Returns true if a packet was consumed.
func _process_one_packet() -> bool:
	var n := _rx_buffer.size()
	if n < 2:
		return false

	var op := _rx_buffer[0]
	var i := 1
	var multiplier := 1
	var remaining := 0
	while true:
		if i >= n:
			return false  # need more bytes
		var b := _rx_buffer[i]
		i += 1
		remaining += (b & 0x7F) * multiplier
		if (b & 0x80) == 0:
			break
		multiplier *= 128
		if multiplier > 128 * 128 * 128:
			push_error("MQTT: malformed remaining-length field")
			_rx_buffer.clear()
			return false

	if n < i + remaining:
		return false  # payload not fully received yet

	_dispatch_packet(op, i, remaining)
	_rx_buffer = _rx_buffer.slice(i + remaining)
	return true


func _dispatch_packet(op: int, p: int, payload_size: int) -> void:
	if op == CP_PINGRESP:
		_log(2, "PINGRESP")
		return

	if (op & 0xF0) == CP_PUBLISH:
		_handle_publish(op, p, p + payload_size)
		return

	if op == CP_CONNACK:
		assert(payload_size == 2, "CONNACK must be 2 bytes")
		var ret := _rx_buffer[p + 1]
		_log(1, "CONNACK return_code=0x%02x" % ret)
		if ret == 0x00:
			_state = State.CONNECTED
			_next_ping_ms = Time.get_ticks_msec() + int(ping_interval * 1000.0)
			broker_connected.emit()
		else:
			_log(0, "MQTT connection refused (code=0x%02x)" % ret)
			_enter_failed()
		return

	if op == CP_PUBACK:
		assert(payload_size == 2, "PUBACK must be 2 bytes")
		var ack_pid := (_rx_buffer[p] << 8) | _rx_buffer[p + 1]
		_log(2, "PUBACK[%d]" % ack_pid)
		publish_acknowledged.emit(ack_pid)
		return

	if op == CP_SUBACK:
		assert(payload_size == 3, "SUBACK must be 3 bytes")
		var ack_pid := (_rx_buffer[p] << 8) | _rx_buffer[p + 1]
		_log(1, "SUBACK[%d] return_code=0x%02x" % [ack_pid, _rx_buffer[p + 2]])
		return

	if op == CP_UNSUBACK:
		assert(payload_size == 2, "UNSUBACK must be 2 bytes")
		var ack_pid := (_rx_buffer[p] << 8) | _rx_buffer[p + 1]
		_log(1, "UNSUBACK[%d]" % ack_pid)
		return

	_log(0, "Unknown MQTT opcode 0x%02x" % op)


func _handle_publish(op: int, p: int, end: int) -> void:
	var topic_len := (_rx_buffer[p] << 8) | _rx_buffer[p + 1]
	var i := p + 2
	var topic := _rx_buffer.slice(i, i + topic_len).get_string_from_utf8()
	i += topic_len

	var qos := (op >> 1) & 0x03
	var pid := 0
	if qos > 0:
		pid = (_rx_buffer[i] << 8) | _rx_buffer[i + 1]
		i += 2

	var data := _rx_buffer.slice(i, end)
	var message = data if binary_messages else data.get_string_from_utf8()
	_log(2, "PUBLISH received topic=%s qos=%d (%d bytes)" % [topic, qos, data.size()])
	received_message.emit(topic, message)

	if qos == 1:
		_send_data(PackedByteArray([CP_PUBACK, 0x02, (pid >> 8) & 0xFF, pid & 0xFF]))
	elif qos == 2:
		# QoS 2 (PUBREC/PUBREL/PUBCOMP) is not implemented.
		push_warning("MQTT: QoS 2 PUBLISH received but QoS 2 is not implemented")

# =============================================================================
# Encoding helpers
# =============================================================================

static func _encode_remaining_length(pkt: PackedByteArray, size: int) -> void:
	assert(size >= 0 and size < 268435456, "MQTT remaining length out of range")
	while true:
		var b := size & 0x7F
		size >>= 7
		if size > 0:
			pkt.append(b | 0x80)
		else:
			pkt.append(b)
			return


static func _encode_short(pkt: PackedByteArray, value: int) -> void:
	assert(value >= 0 and value < 65536, "value out of range for 16-bit field")
	pkt.append((value >> 8) & 0xFF)
	pkt.append(value & 0xFF)


static func _encode_string(pkt: PackedByteArray, bytes: PackedByteArray) -> void:
	_encode_short(pkt, bytes.size())
	pkt.append_array(bytes)


func _coerce_payload(message) -> PackedByteArray:
	if message is PackedByteArray:
		return message
	if message is String:
		return (message as String).to_utf8_buffer()
	return str(message).to_utf8_buffer()

# =============================================================================
# Logging
# =============================================================================

func _log(level: int, message: String) -> void:
	if verbose_level >= level:
		print("[MQTT] ", message)
