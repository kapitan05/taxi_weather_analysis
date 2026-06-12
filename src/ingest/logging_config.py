import json
import logging


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        obj: dict = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            obj["exc"] = self.formatException(record.exc_info)
        obj.update({k: v for k, v in record.__dict__.items() if k not in logging.LogRecord.__dict__ and not k.startswith("_") and k not in {"msg", "args", "exc_info", "exc_text", "stack_info", "lineno", "funcName", "pathname", "filename", "module", "created", "msecs", "relativeCreated", "thread", "threadName", "processName", "process", "name", "levelno", "levelname", "message"}})
        return json.dumps(obj)


def setup_json_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.root.handlers = [handler]
    logging.root.setLevel(level)
