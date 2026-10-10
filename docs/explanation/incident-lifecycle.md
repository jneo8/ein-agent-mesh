# The incident lifecycle

- The states an incident passes through, what each means to the SRE, which topic carries it, and
  which role may move it on.
- How each role does its work is elsewhere: [gateway](gateway.md), [triage](triage.md), and the
  rules all of them follow in [mesh design](mesh-design.md).

```
 Alertmanager
      │ webhook
      ▼
  detected ──triage──▶ open ──orchestrator──▶ investigating ──ask_user──▶ waiting
      │                 ▲ │                        ▲                        │
      │ ignore          └─┘ attach / merge          │  input answered /      │ accept_report:
      ▼                                            │  default, or           │ accept / timeout
    (end)                                          └── send_back ───────────┤
                                                                            ▼
                                                                         closed ──▶ (end)
```

- v1 implements `detected → open` (triage) and `open → investigating` (orchestrator, PoC slice:
  state and event only, no contracts).
- `waiting` and `closed` are defined in the design note and wait on contracts, `ask_user` and the
  frontend.

## States

| State | Meaning to the SRE | Carried on | Moved on by |
| --- | --- | --- | --- |
| detected | Monitoring says something happened. Nobody has decided yet whether it is new, part of something already open, or noise. | `alerts`, one record per alert | triage |
| open | One thing worth investigating, with a `root_id`, a priority and a reason. | `incidents.critical` or `incidents.normal` | triage while it stays open (attach, merge); orchestrator to leave it |
| investigating | Someone is looking into it; there are outstanding contracts. Two investigations that evidence ties together are merged here, never by triage. In the PoC: the orchestrator owns it, nothing is outstanding yet. | `investigation.state` with `status=investigating`, evidence on `investigation.events` | orchestrator |
| waiting | Blocked on a person. The question says which kind: `input` (the investigation needs something only a person knows) or `accept_report` (the final report is posted and awaits sign-off). | `user.questions` (with `kind`, the allowed replies and a timeout default), answered on `user.answers`; `investigation.state` with `status=waiting` | orchestrator, by the question's kind and the reply |
| closed | The SRE accepted the conclusion, or silence past the timeout counted as acceptance. | `investigation.state` with `status=closed` | nobody; terminal |

- An alert triage judges to be noise leaves at `detected` with an `alerts.ignored` event and never
  becomes an incident.

## Transitions

| From | To | Caused by |
| --- | --- | --- |
| detected | open | triage decides `open` |
| detected | open (existing) | triage decides `attach`; the alert joins an incident already open |
| detected | end | triage decides `ignore` |
| open | open | triage decides `merge`; two open incidents become one |
| open | investigating | orchestrator takes the incident (PoC: writes state + `investigation.started`; later: issues the first contract) |
| investigating | investigating | a report arrives; orchestrator delegates again |
| investigating | investigating | evidence links two investigations; orchestrator merges them: the absorbed one gets `status=merged` and `merged_into`, event `investigation.merged`; triage folds its alerts into the survivor |
| investigating | waiting | orchestrator asks a question (`kind=input`), or posts the final report with an `accept_report` question |
| waiting | investigating | `input`: any answer, or the deadline passes and the default is taken. `accept_report`: the SRE replies `send_back` |
| waiting | closed | `accept_report`: the SRE replies `accept`, or the timeout passes |
| any | closed | `@ein close`, a human override not tied to any question |

## Who owns a transition

- Every transition has exactly one writer.
- Into and within `open`: triage. That is `open`, `attach` (an alert joins an open incident) and
  `merge` (two open incidents become one).
- Out of `open` and every state after it: orchestrator.
- Investigators, consultants and the frontend never change a state. They add evidence or relay
  messages.
- Why: [mesh design](mesh-design.md#one-owner-per-transition).

## What ends an investigation

- Closing is the orchestrator's decision. Inputs, in order of preference:
  1. **The SRE**, answering the `accept_report` question: `accept` or `send_back`. Timeout
     defaults to `accept`. The only criterion in v1.
  2. **Alerts resolving.** When every alert of an incident has resolved, triage will emit
     `alerts.resolved` as a hint that it is time to post the report. A hint only.
  3. **A reviewer role**, later, returning a `review` event on the draft report. Evidence for the
     orchestrator, never a state change of its own.
- One waiting state, not two. "Waiting for input" and "waiting for sign-off" differ only in what
  the SRE may reply and where a timeout goes; both are written in the question (`kind`, allowed
  replies, default). "Done but unacknowledged" is read off the open `accept_report` questions, or
  a `report.posted` event, rather than being a state.
- Allowed replies are listed by the question. The frontend does not interpret free text like
  "ok" or "fine for now".

## What the orchestrator remembers

- Everything about one investigation hangs off its `root_id`: the incident, the state snapshot,
  every event, every question and answer, and the frontend thread. Finding "the earlier
  conversation" is a key lookup, never a search.
- `investigation.state` carries a `context` field next to `status`: the checkpoint of what the
  orchestrator needs to resume. Confirmed facts, the current hypothesis and plan, the conclusions of
  questions already answered. The pending question is already in the state.
- `context` is a summary, not a transcript. It is capped at 16 KB; a larger value is rejected by the
  schema and the orchestrator must compress (merge old facts, drop refuted hypotheses) before it
  can write state.
- The raw history stays on `investigation.events`. Kafka cannot fetch a key's records without
  scanning the partition, so a restart reads `investigation.state` to the end and has every
  investigation's status and context back at once. Replaying events is a fallback, not the path.
- Why a bounded summary and not a separate topic: the state record stays a small message, the
  compacted topic stays at roughly active investigations times 16 KB, and there is no second topic
  to fall out of sync. Design note: "調查的記憶：context 欄位".
- Memory across investigations ("like that Ceph case last month") is not this field. It is the
  Outline integration in the design note, out of scope for v1.

## What a closed incident leaves behind

- Its `root_id` on `investigation.state` with `status=closed`.
- Its full history on `investigation.events`, retained 30 days: why it was opened, what was
  attached, every report, who answered what, the final report.
- Nothing in triage's open-incident table. Triage drops the incident when it sees `closed`, so new
  alerts for the same failure open a fresh incident instead of attaching to a closed one.
