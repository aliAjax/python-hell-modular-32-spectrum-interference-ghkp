import json
import mimetypes
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

from .domain import DomainError


def build_handler(service, static_dir):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ModularPythonHell/1.0"

        def log_message(self, fmt, *args):
            return

        def _identity(self):
            actor = self.headers.get("X-User-Id", "").strip()
            role = self.headers.get("X-Role", "").strip()
            region = self.headers.get("X-Region", "").strip() or None
            return actor, role, region

        def _json_body(self):
            length = int(self.headers.get("Content-Length", "0") or "0")
            if not length:
                return {}
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise DomainError("invalid_json", "请求体不是有效 JSON", 400)

        def _send(self, status, value, content_type="application/json; charset=utf-8"):
            if not isinstance(value, (bytes, bytearray)):
                value = json.dumps(value, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(value)))
            self.end_headers()
            self.wfile.write(value)

        def _error(self, exc):
            status = getattr(exc, "status", 500)
            code = getattr(exc, "code", "internal_error")
            self._send(status, {"error": code, "message": str(exc)})

        def do_GET(self):
            try:
                path = urlparse(self.path).path
                if path == "/health":
                    return self._send(200, {"status": "ok"})
                if path == "/api/state":
                    return self._send(200, service.state())
                if path == "/api/items":
                    return self._send(200, {"items": service.list_items()})
                if path == "/api/merge-groups":
                    return self._send(200, {"groups": service.list_merge_groups()})
                parts = [part for part in path.split("/") if part]
                if len(parts) == 4 and parts[:2] == ["api", "merge-groups"] and parts[3] == "candidates":
                    group = service.repository.get_merge_group(parts[2])
                    return self._send(200, group)
                if len(parts) == 3 and parts[:2] == ["api", "merge-groups"]:
                    return self._send(200, service.repository.get_merge_group(parts[2]))
                if len(parts) == 3 and parts[:2] == ["api", "items"]:
                    return self._send(200, service.get_item(int(parts[2])))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "pending-actions":
                    return self._send(200, {"actions": service.repository.list_pending_actions(int(parts[2]))})
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "audit":
                    item = service.get_item(int(parts[2]))
                    return self._send(200, {"events": item["audit"]})
                if path == "/":
                    file_path = os.path.join(static_dir, "index.html")
                    with open(file_path, "rb") as handle:
                        content = handle.read()
                    return self._send(200, content, "text/html; charset=utf-8")
                return self._send(404, {"error": "not_found", "message": "接口不存在"})
            except DomainError as exc:
                return self._error(exc)
            except (ValueError, OSError) as exc:
                return self._error(DomainError("invalid_request", str(exc), 400))

        def do_POST(self):
            actor = role = region = None
            try:
                actor, role, region = self._identity()
                path = urlparse(self.path).path
                payload = self._json_body()
                parts = [part for part in path.split("/") if part]
                if parts == ["api", "items"]:
                    return self._send(201, service.create_item(payload, actor, role, region))
                if parts == ["api", "merge-groups", "rebuild"]:
                    return self._send(200, service.rebuild_pending_groups(actor, role, region))
                if parts == ["api", "offline-sync"]:
                    return self._send(200, service.sync_group(payload, actor, role, region))
                if len(parts) == 5 and parts[:2] == ["api", "merge-groups"] and parts[4] in ("master", "designate"):
                    expected = payload.pop("expected_version", None)
                    return self._send(200, service.designate_master(parts[2], payload, actor, role, region, expected))
                if len(parts) == 4 and parts[:2] == ["api", "pending-actions"] and parts[3] == "execute":
                    pending_id = int(payload.pop("pending_action_id", None) or 0)
                    expected = payload.pop("expected_version", None)
                    return self._send(200, service.execute_pending_action(
                        pending_id, payload, actor, role, expected, region
                    ))
                if len(parts) == 5 and parts[:2] == ["api", "pending-actions"] and parts[4] == "execute":
                    pending_id = int(payload.pop("pending_action_id", None) or parts[2])
                    expected = payload.pop("expected_version", None)
                    return self._send(200, service.execute_pending_action(
                        pending_id, payload, actor, role, expected, region
                    ))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "sources":
                    return self._send(201, service.add_source(int(parts[2]), payload, actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "pending-actions":
                    action = payload.pop("action", "")
                    if not action:
                        raise DomainError("action_required", "缺少 action", 400)
                    return self._send(201, service.queue_action(int(parts[2]), action, payload, actor, role, region))
                if len(parts) == 4 and parts[:2] == ["api", "items"] and parts[3] == "actions":
                    action = payload.pop("action", "")
                    if not action:
                        raise DomainError("action_required", "缺少 action", 400)
                    expected = payload.pop("expected_version", None)
                    return self._send(200, service.act(int(parts[2]), action, payload, actor, role, expected, region))
                return self._send(404, {"error": "not_found", "message": "接口不存在"})
            except DomainError as exc:
                return self._error(exc)
            except Exception as exc:
                return self._error(DomainError("internal_error", str(exc), 500))

    return Handler
