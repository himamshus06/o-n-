
import subprocess
import sys
import json

test_program = """
import sys
import json

database = {}

for line in sys.stdin:
    request = json.loads(line)

    op = request["op"]
    key = request["key"]

    if op == "PUT":
        database[key] = request["value"]
        response = {"id": request["id"], "status": "ACK"}

    elif op == "GET":
        response = {
            "id": request["id"],
            "status": "OK",
            "value": database.get(key)
        }

    elif op == "DELETE":
        database.pop(key, None)
        response = {"id": request["id"], "status": "ACK"}

    print(json.dumps(response), flush=True)
"""

process = subprocess.Popen(
    [sys.executable, "-u", "-c", test_program],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1
)

def send_command(request):
    process.stdin.write(json.dumps(request) + "\n")
    process.stdin.flush()

    response = process.stdout.readline()
    return json.loads(response)

# Test PUT
print("PUT:", send_command({
    "id": 1,
    "op": "PUT",
    "key": "name",
    "value": "hello"
}))

# Test GET
print("GET:", send_command({
    "id": 2,
    "op": "GET",
    "key": "name"
}))

# Test DELETE
print("DELETE:", send_command({
    "id": 3,
    "op": "DELETE",
    "key": "name"
}))

# Test GET after DELETE
print("GET after DELETE:", send_command({
    "id": 4,
    "op": "GET",
    "key": "name"
}))

process.terminate()
process.wait()
