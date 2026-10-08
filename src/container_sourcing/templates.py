"""Pure request-template rendering; no crawler engine or host service imports."""
from __future__ import annotations

import json
import re
from typing import Any

from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

_env = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)
_env.filters['tojson'] = lambda value: json.dumps(value)
_NATIVE = re.compile(r'^\s*\{\{(.*)\}\}\s*$', re.S)


def render(value: Any, scope: dict) -> Any:
    if isinstance(value, str):
        if '{{' not in value and '{%' not in value:
            return value
        match = _NATIVE.match(value)
        if match and '}}' not in match.group(1):
            return _env.compile_expression(match.group(1).strip(), undefined_to_none=False)(**scope)
        return _env.from_string(value).render(**scope)
    if isinstance(value, dict):
        return {key: render(item, scope) for key, item in value.items()}
    if isinstance(value, list):
        return [render(item, scope) for item in value]
    return value
