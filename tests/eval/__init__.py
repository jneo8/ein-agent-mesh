"""Scenario evals on the InMemoryBus: a recorded alert payload goes through gateway mapping, triage
and the orchestrator; the result is scored against an expected grouping.

With the scripted model (default) the score measures the harness and the mesh, not an LLM, and
must be perfect. With ``--llm`` the real model runs and the scripted score is the baseline."""
