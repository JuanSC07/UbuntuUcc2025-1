import csv
import functools
import json
import logging
import os
import pickle
import sys
import threading
from copy import copy
from pathlib import Path
import time
from datetime import datetime

import pika

# CONFIGURACIÓN DE PERSISTENCIA MEJORADA
DATA_DIR = Path("/code/app/data")
JSON_FILE = DATA_DIR / "claims.json"
BACKUP_DIR = DATA_DIR / "backups"

# Crear directorios necesarios
DATA_DIR.mkdir(exist_ok=True, parents=True)
BACKUP_DIR.mkdir(exist_ok=True, parents=True)

# Inicializar archivo JSON con metadatos
if not JSON_FILE.exists():
    initial_data = {
        "metadata": {
            "created_at": datetime.now().isoformat(),
            "version": "1.0",
            "total_records": 0
        },
        "claims": []
    }
    with open(JSON_FILE, "w") as f:
        json.dump(initial_data, f, indent=2)

QUEUE_NAME = os.environ["QUEUENAME"]


def ack_message(channel, delivery_tag):
    """Note that `channel` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass
 


def write_to_json(filename, message: dict, tlock: threading.Lock):
    """Write to a JSON collection file with improved persistence"""
    tlock.acquire()
    try:
        # BACKUP antes de modificar (cada 10 registros)
        if os.path.exists(filename):
            with open(filename, "r") as f:
                try:
                    data = json.load(f)
                    if len(data.get("claims", [])) % 10 == 0:
                        backup_file = BACKUP_DIR / f"claims_backup_{int(time.time())}.json"
                        with open(backup_file, "w") as bf:
                            json.dump(data, bf, indent=2)
                except json.JSONDecodeError:
                    data = {
                        "metadata": {
                            "created_at": datetime.now().isoformat(),
                            "version": "1.0",
                            "total_records": 0
                        },
                        "claims": []
                    }
        else:
            data = {
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": "1.0",
                    "total_records": 0
                },
                "claims": []
            }
        
        # Añadir nuevo registro con timestamp y index
        new_entry = {
            "index": len(data["claims"]),
            "timestamp": datetime.now().isoformat(),
            **message
        }
        data["claims"].append(new_entry)
        
        # Actualizar metadatos
        data["metadata"]["total_records"] = len(data["claims"])
        data["metadata"]["last_updated"] = datetime.now().isoformat()
        
        # ESCRITURA ATÓMICA - escribir a archivo temporal primero
        temp_file = f"{filename}.tmp"
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)
        
        # Mover archivo temporal al definitivo (operación atómica)
        os.replace(temp_file, filename)
        
        logging.info(f"Successfully wrote claim {new_entry['id']} to persistent storage")
        
    except Exception as e:
        logging.error(f"Error writing to JSON file: {e}")
        # En caso de error, intentar escribir a archivo de emergencia
        emergency_file = DATA_DIR / f"emergency_{int(time.time())}.json"
        with open(emergency_file, "w") as f:
            json.dump({"error_message": message, "timestamp": datetime.now().isoformat()}, f)
    finally:
        tlock.release()

def do_work(channel, delivery_tag, body, tlock: threading.Lock):
    """Deserialize the message and write to persistent JSON file."""
    try:
        message = pickle.loads(body)
        
        # Extraer todos los campos incluyendo el nuevo status
        json_message = {
            "id": message.get("id", f"auto_{int(time.time())}"),
            "customer": message.get("customer", "John Doe"),
            "amount": message.get("amount", 500),
            "description": message.get("description", "Car damage claim"),
            "status": message.get("status", "Enviado")
        }
        
        logging.info(f"Processing claim: {json_message['id']}")
        
        # Escribir al archivo JSON PERMANENTE
        write_to_json(JSON_FILE, json_message, tlock)
        
        # Acknowledge message
        cb = functools.partial(ack_message, channel, delivery_tag)
        channel.connection.add_callback_threadsafe(cb)
        
    except Exception as e:
        logging.error(f"Error in do_work: {e}")
        # Rechazar mensaje para reintento
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)



def callback(channel, method_frame, _header_frame, body, args):
    """The callback function when a new message is received"""
    threads, tlock = args
    delivery_tag = method_frame.delivery_tag

    t = threading.Thread(target=do_work, args=(channel, delivery_tag, body, tlock))
    t.start()
    threads.append(t)


def main():
    rabbit_params = pika.ConnectionParameters(host="rabbitmq")
    tlock = threading.Lock()

    # Use context manager to automatically close the connection when the process stops
    with pika.BlockingConnection(rabbit_params) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        channel.basic_qos(prefetch_size=0, prefetch_count=10)

        threads = []
        on_message_callback = functools.partial(callback, args=(threads, tlock))

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message_callback)
        channel.start_consuming()

    # Wait for all to complete
    # Note: may not be called when container is stopped?
    logging.info(f"Number of threads to join: {len(threads)}")
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    # Init regular logging
    ch = logging.StreamHandler(sys.stdout)
    # Change level to [10, 20, 30, 40, 50] for different severity,
    # where 10 is lowest (debug) and 50 is highest (critical)
    logging.basicConfig(
        level=20,
        format="%(asctime)s [%(levelname)s] %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[ch]
    )
    logging.info(f"Starting consumer...")

    sys.exit(main())
