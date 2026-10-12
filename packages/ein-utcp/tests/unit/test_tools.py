"""Operations, catalog lines and the grep over them; GET-only filtering and server URLs."""

import pytest

from ein_utcp.tools import (
    Operation,
    build_catalog,
    catalog_line,
    filter_readonly_operations,
    grep_catalog,
    parse_catalog_line,
    resolve_server_url,
    tool_specs,
)


def op(name="prometheus.query", path="/api/v1/query", props=("time", "query", "pretty"), required=("query",),
       desc="Evaluate | an instant query " + "x" * 80):
    properties = {p: {} for p in props}
    return Operation(name=name, path=path, description=desc, properties=properties,
                     schema={"properties": properties, "required": list(required)})


def test_a_line_has_four_columns_required_first_and_no_pipes_in_text():
    name, path, params, desc = catalog_line(op()).split(" | ")
    assert (name, path, params) == ("prometheus.query", "/api/v1/query", "(query*, time)")  # 'pretty' stripped
    assert "|" not in desc and len(desc) == 60
    back = parse_catalog_line(catalog_line(op()))
    assert back.name == "prometheus.query" and back.path == "/api/v1/query"
    assert back.schema == {"type": "object", "properties": {"query": {"type": "string"}, "time": {"type": "string"}},
                           "required": ["query"]}
    assert parse_catalog_line("a.b | /b | () | none").schema is None


def test_catalog_is_sorted_and_grep_outcomes_are_actionable():
    lines = build_catalog([op("b.z"), op("a.y"), op("a.x", props=(), required=())])
    assert [line.split(" | ")[0] for line in lines] == ["a.x", "a.y", "b.z"]
    assert "a.x | /api/v1/query | () |" in lines[0]
    assert grep_catalog(lines, "A\\.").endswith("[2 matches]")
    assert grep_catalog(lines, "/query \\|", limit=1).endswith("[3 matches, showing first 1 - narrow your pattern]")
    assert grep_catalog(lines, "nothing").startswith("(no matches - widen your pattern)")


@pytest.mark.parametrize("pattern, prefix", [("(", "error: bad pattern"), ("x" * 401, "error: pattern longer")])
def test_bad_patterns_come_back_correctable(pattern, prefix):
    assert grep_catalog(["a | b | () | c"], pattern).startswith(prefix)


def test_tool_specs_follow_the_sources():
    names = [t["function"]["name"] for t in tool_specs(["alertmanager", "prometheus"])]
    assert names == ["grep_operations", "call_operation", "read_result", "grep_result", "refresh_result", "list_results"]
    assert tool_specs(["a"])[1]["function"]["parameters"]["properties"]["source"]["enum"] == ["a"]
    assert tool_specs([]) == []


def test_readonly_filter_and_server_url_resolution():
    spec = {"paths": {"/a": {"get": {}, "post": {}, "parameters": [1]}, "/b": {"delete": {}}, "/c": "junk"}}
    assert filter_readonly_operations(spec, "s")["paths"] == {"/a": {"get": {}, "parameters": [1]}}
    assert resolve_server_url({"basePath": "/api/v2/"}, "http://h/am/") == "http://h/am/api/v2"
    assert resolve_server_url({"servers": [{"url": "/api/v1"}]}, "http://h/p") == "http://h/p/api/v1"
    assert resolve_server_url({"servers": [{"url": "https://elsewhere/"}]}, "http://h/p") == "http://h/p"
    assert resolve_server_url({}, "http://h/l/") == "http://h/l"
