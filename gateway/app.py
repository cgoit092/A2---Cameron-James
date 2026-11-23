import json

import grpc
from flask import Flask, abort, jsonify, request
from werkzeug.exceptions import HTTPException
import event_pb2
import event_pb2_grpc
import registration_pb2
import registration_pb2_grpc

CONFIG = json.load(open("config.json"))
STATUS = {
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
    grpc.StatusCode.FAILED_PRECONDITION: 409,
    grpc.StatusCode.UNIMPLEMENTED: 501,
    grpc.StatusCode.UNAVAILABLE: 503,
}

MAX_INT32 = 2**31 - 1  # largest value an int32 protobuf field can hold

app = Flask(__name__)
events = event_pb2_grpc.EventServiceStub(grpc.insecure_channel(CONFIG["event_service"]))
registrations = registration_pb2_grpc.RegistrationServiceStub(
    grpc.insecure_channel(CONFIG["registration_service"]))


@app.before_request
def check_staff_token():
    protected_routes = {"add_event", "edit_event", "delete_event"}

    if request.endpoint in protected_routes:
        expected_token = f"Bearer {CONFIG['staff_token']}"
        provided_token = request.headers.get("Authorization")

        if provided_token != expected_token:
            abort(401, "Missing or invalid staff token")

def fields(message):
    return {field.name: getattr(message, field.name) for field in message.DESCRIPTOR.fields}

def json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400, "Request body must be a JSON object")
    return body


@app.errorhandler(grpc.RpcError)
def rpc_error(error):
    return jsonify(error=error.details()), STATUS.get(error.code(), 500)


@app.errorhandler(HTTPException)
def http_error(error):
    return jsonify(error=error.description), error.code


@app.get("/api/v1/events")
def list_events():
    reply = events.ListEvents(event_pb2.ListEventsRequest(name=request.args.get("name", "")))
    return jsonify([fields(event) for event in reply.events])


@app.get("/api/v1/events/<int:event_id>")
def get_event(event_id):
    return jsonify(fields(events.GetEvent(event_pb2.EventId(event_id=event_id))))


@app.post("/api/v1/events")
def add_event():
    body = json_body()
    name, capacity = body.get("name"), body.get("capacity")
    if not isinstance(name, str):
        abort(400, "name is required and must be a string")
    if type(capacity) is not int or not 0 <= capacity <= MAX_INT32:
        abort(400, "capacity is required and must be a whole number from 0 to 2147483647")
    event = events.CreateEvent(event_pb2.CreateEventRequest(name=name, capacity=capacity))
    return jsonify(fields(event)), 201


@app.patch("/api/v1/events/<int:event_id>")
def edit_event(event_id):
    body = json_body()
    unknown = set(body) - {"name", "capacity"}
    if unknown:
         abort(400, f"Unknown field(s): {', '.join(sorted(unknown))}")
    if not body:
         abort(400, "Send name and/or capacity")
    if "name" in body and not isinstance(body["name"], str):
         abort(400, "name must be a string")
    if "capacity" in body and (type(body["capacity"]) is not int or not 0 <= body["capacity"] <= MAX_INT32):
         abort(400, "capacity must be a whole number from 0 to 2147483647")
    event = events.UpdateEvent(event_pb2.UpdateEventRequest(event_id=event_id, **body))
    return jsonify(fields(event))


@app.delete("/api/v1/events/<int:event_id>")
def delete_event(event_id):
    events.DeleteEvent(event_pb2.EventId(event_id=event_id))
    return "", 204


@app.post("/api/v1/participants")
def add_participant():
    body = request.get_json(silent=True)

    if not isinstance(body, dict):
        abort(400, "Request body must be a JSON object")

    name = body.get("name")
    if not isinstance(name, str):
        abort(400, "name is required and must be a string")

    participant = registrations.CreateParticipant(
        registration_pb2.CreateParticipantRequest(name=name)
    )

    return jsonify(fields(participant)), 201


@app.get("/api/v1/participants/<int:participant_id>")
def get_participant(participant_id):
    participant = registrations.GetParticipant(
        registration_pb2.ParticipantId(participant_id=participant_id)
    )

    return jsonify(fields(participant))


@app.get("/api/v1/events/<int:event_id>/registrations")
def list_bookings(event_id):
    reply = registrations.ListConfirmedBookings(
        registration_pb2.EventRegistrationsRequest(event_id=event_id)
    )
    return jsonify([fields(booking) for booking in reply.bookings])


@app.post("/api/v1/registrations")
def book():
    abort(501, "TODO task 4: implement POST /api/v1/registrations")


@app.delete("/api/v1/registrations/<int:booking_id>")
def cancel(booking_id):
    registrations.CancelBooking(registration_pb2.BookingId(booking_id=booking_id))
    return "", 204


app.run(host=CONFIG["host"], port=CONFIG["port"])
