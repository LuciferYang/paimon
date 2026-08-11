# Cross-version binary linkage check

`paimon-spark-common` and `paimon-spark4-common` are each compiled **once**, against the
newest Spark the `spark4` profile supports, and the resulting classfiles are shipped to every
older 4.x runtime. `check_linkage.py` answers the question the build cannot: do those
classfiles actually *link* against the older Spark?

## Why a compile is not enough

Two ways a bump breaks older runtimes while every module still compiles:

- **class ⇄ interface flip.** A member keeps its exact signature but its owner changes kind,
  so the compiler emits `invokeinterface` where the old Spark needs `invokevirtual`. The JVM
  raises `IncompatibleClassChangeError`. Spark 4.2 did this to `CatalogManager` — verified in
  both directions, so it is not a "new Spark is a superset" situation.
- **renamed or removed member.** `NoSuchMethodError` / `NoSuchFieldError`. Spark 4.2 renamed
  `RewriteRowLevelCommand`'s three `DELTA_OPERATIONS_WITH_*` to `OPERATIONS_WITH_*` and
  widened `AliasHelper.getAliasMap(Seq)` to `getAliasMap(Iterable)`.

Both surface at **constant-pool resolution**, which HotSpot performs lazily and per call
site. A test suite only catches them on paths it executes, and even `Class.forName` on every
class does not force resolution. Static scanning is the only complete check.

## Usage

```bash
# assemble one directory of jars per Spark version (symlinks are fine)
M2=~/.m2/repository/org/apache/spark
for v in 4.0.3 4.1.2 4.2.0; do
  mkdir -p /tmp/sparkjars/$v
  find $M2 -name "*-$v.jar" | grep -v sources | grep -v javadoc |
    while read j; do ln -sf "$j" /tmp/sparkjars/$v/; done
done

SCALA_LIB=$(ls ~/.m2/repository/org/scala-lang/scala-library/2.13.*/scala-library-2.13.*.jar | tail -1)

# target/classes must come from a `-Pspark4` build; see the warning below
mvn -B -ntp -Pspark4 -DskipTests clean install \
  -pl paimon-spark/paimon-spark-common,paimon-spark/paimon-spark4-common

python3 tools/spark-binary-compat/check_linkage.py \
  --classes paimon-spark/paimon-spark-common/target/classes,paimon-spark/paimon-spark4-common/target/classes \
  --target-jars /tmp/sparkjars/4.0.3 --extra-cp "$SCALA_LIB" --label "Spark 4.0.3"
```

Exit status is 1 when anything is reported, so it drops into CI as a gate.

**Build under the profile you are scanning for.** `paimon-spark-common` is built by both
profiles into the same `target/classes`, so a preceding `-Pspark3` build leaves Scala 2.12
classfiles there. Scanning those against 2.13 Spark jars reports every `Seq`-typed member as
missing — `scala.collection.Seq` in 2.12 versus `scala.collection.immutable.Seq` in 2.13 — which
looks like dozens of findings and is entirely an artifact. Tell them apart by the descriptors:
`Lscala/collection/Seq;` in the output means the classes are 2.12.

`--extra-cp` matters: Spark's class hierarchy inherits from Scala types, and a supertype the
scan cannot open is a blind spot rather than a clean result. Omitting it costs real findings —
`CatalogStorageFormat$` extends only `java.io.Serializable`, so without the Scala library on
the classpath the walk stops one step in. Uninspectable supertypes are reported as a caveat
on stderr; treat a non-empty caveat as reduced coverage, not as a pass.

Before trusting a run, execute `self_test.sh` with the same three jar directories. It pins the
scanner's three judgement calls to hand-verified classes:

```bash
bash tools/spark-binary-compat/self_test.sh \
  /tmp/sparkjars/4.0.3 /tmp/sparkjars/4.1.2 /tmp/sparkjars/4.2.0
```

## Reading the output

| Finding | Runtime failure |
|---|---|
| `KIND_FLIP interface->class` | `IncompatibleClassChangeError` |
| `KIND_FLIP class->interface` | `IncompatibleClassChangeError` |
| `MISSING_CLASS` | `NoClassDefFoundError` |
| `MISSING_METHOD` | `NoSuchMethodError` |
| `MISSING_FIELD` | `NoSuchFieldError` |

A finding says the reference **cannot resolve**, not that anything reaches it. Triage each
one; do not read the count as a verdict. Two recurring shapes are genuinely harmless and are
already filtered out:

- **Scala mixin artifacts** — `invokespecial Trait.member` (diamond-disambiguating override)
  and `invokestatic Trait.member$` (default-method forwarder). The compiler writes these on
  Paimon's behalf to satisfy an inherited member. If the target Spark never declares that
  member, the inheritance requiring it does not exist there either, so the generated body is
  unreachable. `PaimonSparkTableBase`'s `reportDriverMetrics` is the canonical case: it exists
  only because Spark 4.2 put the member on *both* `StagedTable` and `TruncatableTable`.
- **`java.lang.Object` members and non-Spark supertypes** — `getClass`, `equals`, Scala
  collections, Hadoop. A Spark bump cannot have moved them.

## Baseline diffing

To see only what *this* bump introduces, record the previous baseline's findings and subtract:

```bash
# on the pre-bump commit, having built with the old baseline
python3 check_linkage.py --classes ... --target-jars ... --emit-baseline /tmp/base-40.txt

# on the bumped commit
python3 check_linkage.py --classes ... --target-jars ... --baseline /tmp/base-40.txt
```

Finding keys are `(kind, owner, member, descriptor)` — stable across builds, so the diff is
meaningful even when unrelated code moves.

## Fixing what it reports

Two options, in order of preference:

1. **Neutralize.** Reach the member through a named accessor that both versions answer
   identically, or — when the incompatibility is in the *static type* rather than the arity —
   through `SparkVersionCompat`, which resolves reflectively at runtime. Reflection is immune
   to the class/interface flip: only invoke opcodes carry the distinction, `Class.getMethod`
   does not.
2. **Fork the file** into the per-baseline module (`paimon-spark4-common-4.0`), where a copy
   with the same FQCN is compiled against the older Spark and wins by shade ordering.

Prefer (1): it keeps one copy of the code. Reach for (2) when the two versions need genuinely
different logic, not just a different way of spelling the same call.
