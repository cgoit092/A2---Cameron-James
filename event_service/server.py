# Python 3.14 removed old TLS names that mysql-connector 9.5 still expects; add them back.
import ssl
for _name in ("PROTOCOL_TLSv1", "PROTOCOL_TLSv1_1", "PROTOCOL_TLSv1_2"):
    if not hasattr(ssl, _name):
        setattr(ssl, _name, ssl.PROTOCOL_TLS_CLIENT)

import json
from concurrent import futures

import grpc
import mysql.connector
import event_pb2 as pb
import event_pb2_grpc as rpc

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


class EventService(rpc.EventServiceServicer):
    def GetEvent(self, request, context):
        found = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                        FROM events WHERE event_id = %s""", (request.event_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Event not found")
        return pb.Event(**found[0])

    def ReleaseSeat(self, request, context):
        self.GetEvent(request, context)
        write("UPDATE events SET seats_taken = seats_taken - 1 WHERE event_id = %s", (request.event_id,))
        return pb.Empty()

    def ReserveSeat(self, request, context):
        with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
            cursor.execute(
                "UPDATE events SET seats_taken = seats_taken + 1 "
                "WHERE event_id = %s AND seats_taken < capacity",
                (request.event_id,)
            )
            reserved = cursor.rowcount
            db.commit()

        if reserved == 0:
            self.GetEvent(request, context)
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Event is full")

        return pb.Empty()

    def ListEvents(self, request, context):
        rows = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                       FROM events WHERE name LIKE %s ORDER BY event_id""",
                    ("%" + request.name + "%",))
        return pb.EventList(events=[pb.Event(**row) for row in rows])

    def check_name(self, name, context):
        name = name.strip()
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Event name must not be blank")
        if len(name) > 150:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Event name must be 150 characters or fewer")
        return name

    def CreateEvent(self, request, context):
        name = self.check_name(request.name, context)
        new_id = write("INSERT INTO events (name, capacity) VALUES (%s, %s)", (name, request.capacity))
        return self.GetEvent(pb.EventId(event_id=new_id), context)

    def UpdateEvent(self, request, context):
        found = read("SELECT seats_taken FROM events WHERE event_id = %s", (request.event_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Event not found")
        changes, args = [], []
        if request.HasField("name"):
            changes.append("name = %s")
            args.append(self.check_name(request.name, context))
        if request.HasField("capacity"):
            if request.capacity < found[0]["seats_taken"]:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "Capacity cannot be lower than the seats already booked")
            changes.append("capacity = %s")
            args.append(request.capacity)
        if changes:
            write(f"UPDATE events SET {', '.join(changes)} WHERE event_id = %s", (*args, request.event_id))
        return self.GetEvent(pb.EventId(event_id=request.event_id), context)

    def DeleteEvent(self, request, context):
        found = read("SELECT seats_taken FROM events WHERE event_id = %s", (request.event_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Event not found")
        if found[0]["seats_taken"] > 0:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Event has bookings and cannot be deleted")
        write("DELETE FROM events WHERE event_id = %s", (request.event_id,))
        return pb.Empty()

server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
rpc.add_EventServiceServicer_to_server(EventService(), server)
server.add_insecure_port(CONFIG["listen"])
server.start()
print(f"Event service on {CONFIG['listen']}", flush=True)
server.wait_for_termination()
