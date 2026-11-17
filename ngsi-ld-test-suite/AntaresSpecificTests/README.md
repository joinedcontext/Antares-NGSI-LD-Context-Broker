# Antares specific tests

Robot tests for behaviour Antares defines for itself.

`TP/` is the ETSI conformance suite: every test there asserts a SHALL that can
be pointed at in ETSI GS CIM 009, and the suite is run against other brokers
during interoperability campaigns. A test asserting behaviour no clause
mandates does not belong in it — it would fail every other conformant broker
and read as a conformance claim it is not.

That behaviour lives here instead. Every ETSI cell runs this folder as its
own suite entry, `AntaresSpecificTests` in `dev/etsi-suites.sh`, reported on
its own row apart from the `TP/` suites. A file that needs a specially built
broker is listed in `ANTARES_SPECIFIC_ELSEWHERE` with the workflow that runs
it instead (`policy_engine.robot`: the examples workflow, on the reference
policy engine).

Every test here states in its documentation that it is an Antares decision
rather than a CIM 009 requirement, and why the behaviour exists.

## Running

Against a broker started per the local recipe:

```
/workspace/.venv/bin/robot --variable url:http://localhost:9090/ngsi-ld/v1 \
  --outputdir /tmp/antares-specific AntaresSpecificTests/
```

Run from the suite root, so `${EXECDIR}` resolves `resources/`.
