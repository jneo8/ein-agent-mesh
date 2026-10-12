# ein-utcp

A read-only tool plane for an investigator agent: the model gets to look at the monitoring systems, and nothing it does there can change them.

## What this is for

An investigator is handed an incident and asked for a probable cause. To say anything it has to look: which alerts are firing right now, what a metric did in the last hour, what the logs of one pod say. Those answers live in Prometheus, Loki and Alertmanager. This package lets a model query them, with three things settled up front.

**Only reading exists.** Each system's API is described by an OpenAPI spec shipped here. When a spec is loaded, everything that is not a GET is dropped before it can become a tool. A write operation is not forbidden, it is absent, so a misled or mistaken model has nothing to call. This holds regardless of what the service account behind the endpoint would permit.

**A result never floods the context.** A list of three hundred alerts is tens of kilobytes. Instead of truncating it or pasting it whole, the model receives a digest computed over every record: how many, the distribution of the fields that have few distinct values, a few samples, and a handle. With the handle it can read a window, project just the fields it needs, grep for a record, or re-run the call. The full result stays here, cached; the model pulls what it needs in bounded pieces.

**The runtime keeps the evidence.** Every call that actually went out is recorded, with its arguments, in the order it happened. A report's "resources checked" comes from that record, not from what the model says about itself.

## How it works

The model sees a small, fixed set of tools and a catalog: one line per operation, with its path, parameters and a short description, small enough to sit in the system prompt. It calls an operation by name with its parameters as name/value pairs. Before the call leaves, the parameters are checked against the operation's schema (a misspelled or missing one comes back as a correctable message, not a distant HTTP error), a query is checked by the server's own parser, and a call identical to one already registered is refused in favour of the handle it already has. Then the call goes out over HTTP, the answer is snapshotted under a handle, and the digest goes back. Every failure, including HTTP errors and timeouts, is a readable error payload; nothing raises into the loop.

Each system is a *source*, and a source decides for itself how each of those steps works for it: which operations it has, what to check before a call, how to send it, and how to find the records in what comes back. The shared code is the contract and a default for the common case (a shipped spec over HTTP). Prometheus and Loki add a syntax check through their `format_query` endpoint and know their records sit under `data.result`; Alertmanager knows its alerts come back as a bare array. A change for one system is made in that system's module and cannot reach another.

## What is not here

No message bus and no model client. The loop that calls a model, feeds it tool results and turns its answer into a report lives in the consumer. A scripted source with canned responses walks the same steps without a network, so that loop can be tested and evaluated here too.

## Specs

The specs are files in the repository, not fetched at runtime. `just download` in this package refreshes the ones that have an upstream (Prometheus from a pinned commit, Alertmanager from a release tag); the Loki spec is written by hand because Loki publishes none. A new version is a new file next to the old one.

## Adding a source

Put the system's OpenAPI spec under `specs/<name>/<version>.yaml` and a module under `sources/` with a class extending `HttpSource`. Set its name and default spec version; override only the steps where the system differs from the default. Register it in `sources/__init__.py`. Another version of the spec is another file in the same directory, chosen per deployment through the source's config.
