# The mesh in two minutes

## What it is

A set of SRE agents that turn a flood of alerts into a handful of investigated incidents. The
agents never call each other. Each one reads a stream, decides, and writes to the streams it owns.
Kafka is the backbone between them: it carries the work, keeps every agent's memory, and is the
only thing an agent needs to be present. Everything an agent touches in the infrastructure is
read-only; fixing things stays with people.

## The shape of the problem

One failure produces dozens of alerts across layers. Nobody wants dozens of investigations, and
nobody wants one investigation that hides a second failure inside the first. So the mesh does three
things in order: it decides what the alerts *are* (noise, part of something already open, or new),
it investigates what is new, and it tells a person what it found and asks what to do.

## Two kinds of stream

- **Flow.** A message travelling from one agent to the next: an alert, an incident, a task, a
  report. Written by one agent, read by one agent, gone when consumed.
- **Memory.** What one agent needs to remember to pick up where it left off: the incidents a
  cluster currently has open, where each investigation stands. Owned by exactly one agent, rewritten
  whole, read only by that agent when it restarts.

Nothing else exists. There is no shared log that everybody writes and nobody reads; the reason behind a
decision lives in the memory of the agent that made it. Everything about one incident shares one
identity, so it always travels together.

## The agents

- **Gateway** turns monitoring webhooks into alerts. No judgement.
- **Triage** decides what each alert means. Rules handle what is already known: known noise is
  dropped, an alert whose group already has one open incident joins it. The model is asked only about
  what is new, in one batch per cluster: open or ignore, with a title and a priority. It groups by
  what the labels show and never guesses at causes across layers; a wrong merge hides a failure, a
  wrong split only costs one extra investigation.
- **Orchestrator** owns every incident from the moment it is open. It hands out tasks, folds the
  reports into a bounded summary of what is known, decides what to do next, and is the only agent
  that talks to people.
- **Investigator** does one task at a time: look, do not touch, report what you found with the
  evidence. It has no memory and makes no decisions about the incident. Today it reasons over the
  quoted alerts; read-only tools come next.
- **Frontend** (later) relays questions to a chat thread and answers back. It translates; it never
  decides.

## The lifecycle

An incident is *detected*, then *open*, then *investigating*, then *waiting* on a person, then
*closed*. Every step has exactly one agent allowed to make it: triage opens, the orchestrator does
everything after. This is what keeps two agents from working on the same thing or looping on each
other.

## What we hold on to

- One owner per decision, one identity per incident.
- The model is used only where rules cannot do the job, and its answer is checked before it is trusted;
  a rule-based fallback always exists.
- An agent's memory is small and bounded; it is a summary of what matters, never a transcript.
- Work is never edited, only re-issued with a pointer to what it replaces.
- People are asked, never bypassed: closing an incident is a person's call.

Where the details are: [lifecycle](lifecycle.md), [design rules](design-rules.md),
[triage](triage.md), [topics](../reference/topics.md), [commands](../reference/cli.md).
