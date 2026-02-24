import zmq
import json
import signal
import sys

SEARCH_PORT = 5554
DB_SERVICE_ADDRESS = "tcp://localhost:5556"
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 10


class SearchService:

    def __init__(self):
        self.context = zmq.Context()

        self.server_socket = self.context.socket(zmq.REP)
        self.server_socket.bind(f"tcp://*:{SEARCH_PORT}")


        self.db_socket = self.context.socket(zmq.REQ)
        self.db_socket.connect(DB_SERVICE_ADDRESS)

        print(f"Search Service running on port {SEARCH_PORT}")
        print(f"Connected to DB service at {DB_SERVICE_ADDRESS}")

        signal.signal(signal.SIGINT, self.shutdown)


    def shutdown(self, sig, frame):
        print("\nShutting down Search Service...")
        self.server_socket.close()
        self.db_socket.close()
        self.context.term()
        sys.exit(0)


    def validate_request(self, req):
        if "action" not in req:
            raise ValueError("Missing action field")

        if req["action"] == "search":
            if "table" not in req:
                raise ValueError("Missing table name")

    def call_database(self, payload):
        self.db_socket.send(json.dumps(payload).encode("utf-8"))
        response = self.db_socket.recv()
        return json.loads(response.decode("utf-8"))


    def handle_search(self, req):

        table = req["table"]
        filters = req.get("filters", {})
        page = max(1, int(req.get("page", 1)))
        page_size = min(int(req.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)

        # Call DB microservice
        db_request = {
            "action": "select",
            "table": table,
            "filters": filters
        }

        db_response = self.call_database(db_request)

        if db_response["status"] != "success":
            raise Exception("Database service error")

        rows = db_response["data"]["rows"]

        total_count = len(rows)
        start = (page - 1) * page_size
        end = start + page_size

        paginated_rows = rows[start:end]

        return {
            "status": "success",
            "data": paginated_rows,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_count,
                "total_pages": (total_count + page_size - 1) // page_size
            }
        }


    def route(self, req):
        action = req["action"]

        if action == "search":
            return self.handle_search(req)


        else:
            raise ValueError(f"Unknown action: {action}")


    def run(self):
        while True:
            try:
                message = self.server_socket.recv()
                request = json.loads(message.decode("utf-8"))

                self.validate_request(request)

                response = self.route(request)

            except Exception as e:
                response = {
                    "status": "error",
                    "message": str(e)
                }

            self.server_socket.send(json.dumps(response).encode("utf-8"))


if __name__ == "__main__":
    service = SearchService()
    service.run()
