# Python 3.14 removed old TLS names that mysql-connector 9.5 still expects; add them back.
import ssl
for _name in ("PROTOCOL_TLSv1", "PROTOCOL_TLSv1_1", "PROTOCOL_TLSv1_2"):
    if not hasattr(ssl, _name):
        setattr(ssl, _name, ssl.PROTOCOL_TLS_CLIENT)

import json
from concurrent import futures

import grpc
import mysql.connector
import event_pb2
import event_pb2_grpc
import registration_pb2 as pb
import registration_pb2_grpc as rpc

CONFIG = json.load(open("config.json"))


def read(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor(dictionary=True) as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall()


def write(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
        cursor.execute(sql, args)
        db.commit()
        return cursor.lastrowid


def call(method, request, context):
    try:
        return method(request)
    except grpc.RpcError as error:
        context.abort(error.code(), error.details())


class RegistrationService(rpc.RegistrationServiceServicer):
    def __init__(self, events):
        self.events = events

    def CancelBooking(self, request, context):
        found = read("SELECT event_id, state FROM bookings WHERE booking_id = %s", (request.booking_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Booking not found")
        if found[0]["state"] == "confirmed":
            write("UPDATE bookings SET state = 'cancelled' WHERE booking_id = %s", (request.booking_id,))
            call(self.events.ReleaseSeat, event_pb2.EventId(event_id=found[0]["event_id"]), context)
        return pb.Empty()

    def GetParticipant(self, request, context):
        rows = read(
            "SELECT participant_id, name FROM participants WHERE participant_id = %s",
            (request.participant_id,)
        )
        if not rows:
            context.abort(grpc.StatusCode.NOT_FOUND, "Participant not found")

        return pb.Participant(**rows[0])

    def CreateParticipant(self, request, context):
        name = request.name.strip()
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Participant name must not be blank")
        if len(name) > 150:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Participant name must be 150 characters or fewer")
        new_id = write(
            "INSERT INTO participants (name) VALUES (%s)",
            (name,)
        )

        return self.GetParticipant(pb.ParticipantId(participant_id=new_id), context)

    def CreateBooking(self, request, context):
        event_id = request.event_id
        participant_id = request.participant_id

        if event_id <= 0 or participant_id <= 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Event ID and participant ID must be positive"
            )

        participant = self.GetParticipant(
            pb.ParticipantId(participant_id=participant_id), context
        )

        event = event_pb2.EventId(event_id=event_id)
        call(self.events.GetEvent, event, context)

        existing = read(
            """SELECT booking_id FROM bookings
               WHERE event_id = %s AND participant_id = %s
               AND state = 'confirmed'""",
            (event_id, participant_id)
        )

        if existing:
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Participant already booked for this event"
            )

        call(self.events.ReserveSeat, event, context)

        try:
            booking_id = write(
                """INSERT INTO bookings (event_id, participant_id)
                   VALUES (%s, %s)""",
                (event_id, participant_id)
            )
        
        except mysql.connector.Error as error:
            try:
                self.events.ReleaseSeat(event)
            except grpc.RpcError:
                print("Warning: Failed to release seat after booking error")

            if error.errno == 1062:
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    "Participant already booked for this event"
                )

            context.abort(
                grpc.StatusCode.INTERNAL,
                "Failed to save booking"
            )


        return pb.Booking(
            booking_id=booking_id,
            event_id=event_id,
            participant_id=participant_id,
            participant_name=participant.name
        )


events = event_pb2_grpc.EventServiceStub(grpc.insecure_channel(CONFIG["event_service"]))
server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
rpc.add_RegistrationServiceServicer_to_server(RegistrationService(events), server)
server.add_insecure_port(CONFIG["listen"])
server.start()
print(f"Registration service on {CONFIG['listen']}", flush=True)
server.wait_for_termination()
