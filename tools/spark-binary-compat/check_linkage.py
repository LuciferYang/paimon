#!/usr/bin/env python3
##########################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################
"""Static linkage scan: will classfiles compiled against Spark X resolve against Spark Y?

WHY THIS EXISTS
---------------
`paimon-spark-common` / `paimon-spark4-common` are compiled ONCE against the newest
supported Spark and the resulting classfiles are shipped to older Spark runtimes.
Successful compilation says nothing about that working. Two ways it breaks:

  * a member keeps its exact signature while its owner flips class <-> interface, which
    changes the emitted opcode (`invokevirtual` <-> `invokeinterface`) and yields
    `IncompatibleClassChangeError`. Spark 4.2 did this to `CatalogManager`.
  * a member is renamed, removed, or gains a parameter -> `NoSuchMethodError` /
    `NoSuchFieldError`. A Scala case class gaining a field is the common shape: a call that
    omits the new argument compiles to `apply$default$N`, absent on the older version.

Both surface at *constant-pool resolution* time, which HotSpot does lazily, per call site.
So neither a compile nor a "load every class" smoke test finds them -- only a static scan
of the constant pool does.

BASELINE DIFF
-------------
Some findings predate the bump: `paimon-spark-common` has always been compiled against one
Spark and shipped to others, so master already carries a few. Pass --baseline to subtract a
scan of the same sources built against the previous baseline; what remains is what THIS bump
introduces. Both sets deserve attention, but only the delta blocks the bump.

LIMITS
------
Reports resolution errors, not reachability: a finding on a code path no runtime reaches is
harmless. Triage each one; do not treat the count as a verdict. See README.md for the two
noise shapes that are filtered out and why, and self_test.sh for the cases pinning that
behaviour.
"""

import argparse
import concurrent.futures as cf_futures
import os
import re
import subprocess
import sys
import zipfile
from collections import defaultdict

# javap prints the opcode and the resolved constant-pool entry on ONE line, e.g.
#   12: invokeinterface #96,  1  // InterfaceMethod org/apache/spark/.../CatalogManager.catalog:(...)...
#    1: invokespecial  #43       // InterfaceMethod org/apache/spark/.../TruncatableTable.reportDriverMetrics:()...
#    4: invokestatic   #77       // InterfaceMethod org/apache/spark/.../AliasHelper.getAliasMap$:(...)...
# Capturing both together is what lets a real call be told apart from a Scala mixin artifact.
# `Method` vs `InterfaceMethod` is separately the class/interface flip signal.
REF_RE = re.compile(
    r"(?:(\w+)\s+#[\d,\s]+)?//\s+(Method|InterfaceMethod|Field)\s+"
    r"(org/apache/spark/[\w/$]+)\.([\w$<>]+):(\S+)"
)


# Every class inherits these, and javap does not print `extends java.lang.Object`, so the
# supertype walk would otherwise report them as missing on any Spark type.
OBJECT_MEMBERS = {
    ("getClass", "()Ljava/lang/Class;"),
    ("hashCode", "()I"),
    ("equals", "(Ljava/lang/Object;)Z"),
    ("toString", "()Ljava/lang/String;"),
    ("clone", "()Ljava/lang/Object;"),
    ("notify", "()V"),
    ("notifyAll", "()V"),
    ("wait", "()V"),
    ("wait", "(J)V"),
    ("wait", "(JI)V"),
    ("finalize", "()V"),
}


def javap(*args):
    p = subprocess.run(["javap", *args], capture_output=True, text=True)
    return p.stdout


def strip_generics(s):
    """Drops `<...>` groups so a javap header line can be split on extends/implements.

    javap prints `extends QueryPlan<LogicalPlan> implements AnalysisHelper, ...`; a type
    argument can itself be generic, so this tracks depth rather than regex-matching.
    """
    out, depth = [], 0
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    return "".join(out)


def parse_header(line):
    """-> (kind, [internal names of direct supertypes]) from a javap declaration line."""
    s = strip_generics(line).rstrip("{ ").strip()
    kind = "interface" if re.search(r"\binterface\s", s) else "class"
    supers = []
    m = re.search(r"\bimplements\s+(.+)$", s)
    if m:
        supers += [x.strip() for x in m.group(1).split(",")]
        s = s[: m.start()]
    m = re.search(r"\bextends\s+(.+)$", s)
    if m:
        supers += [x.strip() for x in m.group(1).split(",")]
    return kind, [x.replace(".", "/") for x in supers if x]


def list_class_files(dirs):
    out = []
    for d in dirs:
        for root, _, files in os.walk(d):
            out += [os.path.join(root, f) for f in files if f.endswith(".class")]
    return sorted(out)


class SparkIndex:
    """Indexes a Spark distribution: per class, its kind and declared members.

    Also resolves types *outside* Spark that Spark's hierarchy inherits from
    (`java.io.Serializable`, `scala.Product`, ...), because member resolution has to walk
    through them. javap finds JDK classes with no classpath; anything else needs `extra_cp`.
    """

    def __init__(self, jars, extra_cp=None):
        self.entry_to_jar = {}
        for j in jars:
            try:
                with zipfile.ZipFile(j) as z:
                    for n in z.namelist():
                        if n.endswith(".class"):
                            self.entry_to_jar.setdefault(n, j)
            except zipfile.BadZipFile:
                continue
        self.fallback_cp = os.pathsep.join(jars + ([extra_cp] if extra_cp else []))
        self.unknown = set()
        self._cache = {}

    def has_class(self, name):
        return name + ".class" in self.entry_to_jar

    def info(self, name):
        if name in self._cache:
            return self._cache[name]
        jar = self.entry_to_jar.get(name + ".class")
        txt = javap("-p", "-s", "-cp", jar or self.fallback_cp, name.replace("/", "."))
        if not txt.strip():
            # Neither in Spark nor reachable via the JDK / extra classpath. Recorded so the
            # report can say how much of the hierarchy it could not see.
            self.unknown.add(name)
            self._cache[name] = None
            return None
        kind, supers, members, pending = "class", [], set(), None
        header = False
        for raw in txt.splitlines():
            s = raw.strip()
            if not header and s.endswith("{") and re.search(r"\b(class|interface)\b", s):
                kind, supers = parse_header(s)
                header = True
                continue
            if s.startswith("descriptor:"):
                if pending:
                    members.add((pending, s.split(":", 1)[1].strip()))
                    pending = None
                continue
            m = re.match(r"^.*?([\w$<>]+)\s*\(.*\).*;$", s) or re.match(r"^.*?\b([\w$]+);$", s)
            if m:
                pending = m.group(1)
        info = dict(kind=kind, supers=supers, members=members)
        self._cache[name] = info
        return info

    def resolves(self, owner, name, desc, seen=None):
        """Walks the supertype chain like JVM member resolution (JVMS 5.4.3.3/5.4.3.4).

        Two things the walk cannot read off javap output:

        * every type inherits `java.lang.Object`'s members, but javap never prints that
          supertype, so `Object`'s members are hard-coded.
        * a supertype neither in Spark nor reachable via the JDK / `extra_cp` cannot be
          inspected at all. Those references get the benefit of the doubt, and the owner is
          recorded in `self.unknown` so the report can name the blind spot. Treating "left
          the Spark jars" as "resolvable" instead would swallow real findings -- that is how
          a missing `CatalogStorageFormat$.apply$default$7` escaped an earlier revision of
          this scan, since that companion's only supertype is `java.io.Serializable`.
        """
        if (name, desc) in OBJECT_MEMBERS:
            return True
        seen = seen or set()
        if owner in seen:
            return False
        seen.add(owner)
        info = self.info(owner)
        if info is None:
            return True  # uninspectable supertype; see docstring
        if (name, desc) in info["members"]:
            return True
        return any(self.resolves(s, name, desc, seen) for s in info["supers"])


# One `javap` process per classfile dominates wall clock, and javap only labels which input a
# line came from under `-verbose`, so batching would cost the per-file attribution needed for
# triage. Run per file, concurrently instead: the work is process-bound, so threads suffice.
WORKERS = max(4, (os.cpu_count() or 4))


def refs_of(class_file):
    """-> (class_file, {(kind, owner, name, desc, is_mixin_artifact), ...})

    `is_mixin_artifact` marks a reference the Scala compiler emitted on Paimon's behalf, not
    one Paimon's source contains:

      * `invokespecial Trait.member` -- a diamond-disambiguating override. Scala must pick a
        winner when two mixed-in traits both provide `member`; the override's whole body is
        this one call.
      * `invokestatic Trait.member$` -- a default-method forwarder. The forwarder *is* the
        class's `member()`, so it only runs if something calls `member()`.

    In both cases, if the target Spark does not declare `member`, nothing there can reach the
    generated body: the trait it satisfies has no such member to dispatch through, and a
    Paimon-side call would show up separately as a call on the Paimon type.

    `$init$` is emphatically NOT in that category even though it matches the `member$` shape:
    it is the trait initializer, invoked unconditionally from the implementing class's
    constructor. If the target Spark's trait has no `$init$`, every instantiation throws.
    """
    txt = javap("-c", "-p", class_file)
    refs = set()
    for op, kind, owner, name, desc in REF_RE.findall(txt):
        artifact = name != "$init$" and (
            op == "invokespecial" or (op == "invokestatic" and name.endswith("$"))
        )
        refs.add((kind, owner, name, desc, artifact))
    return class_file, refs


def refs_by_class(class_files):
    """-> {classfile: {(kind, owner, name, desc, is_mixin_artifact), ...}}"""
    with cf_futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return dict(pool.map(refs_of, class_files))


def scan(class_dirs, jars, own_classes, extra_cp=None):
    """-> (findings, unknown_supertypes)

    `findings` maps a stable `(kind, owner, member, descriptor)` key to the classfiles that
    reference it. `unknown_supertypes` names types the walk could not inspect, so the caller
    can report how much of the hierarchy went unchecked.
    """
    idx = SparkIndex(jars, extra_cp=extra_cp)
    findings = defaultdict(list)
    for cf, refs in refs_by_class(list_class_files(class_dirs)).items():
        for kind, owner, name, desc, artifact in refs:
            # Paimon plants its own classes in `org.apache.spark.*` (e.g. `PaimonUtils`,
            # `Spark4Shim`) to reach package-private Spark internals. Those live in the module
            # under scan, not in Spark, so skip them.
            if owner in own_classes or owner.rstrip("$") in own_classes:
                continue
            if not idx.has_class(owner):
                key = ("MISSING_CLASS", owner, "", "")
            else:
                target_kind = idx.info(owner)["kind"]
                if kind == "Method" and target_kind == "interface":
                    key = ("KIND_FLIP class->interface", owner, name, desc)
                elif kind == "InterfaceMethod" and target_kind == "class":
                    key = ("KIND_FLIP interface->class", owner, name, desc)
                elif not idx.resolves(owner, name, desc):
                    if artifact:
                        continue  # see `refs_of`: compiler-emitted, unreachable on target
                    key = ("MISSING_" + ("FIELD" if kind == "Field" else "METHOD"), owner, name, desc)
                else:
                    continue
            findings[key].append(os.path.basename(cf))
    return findings, idx.unknown


def own_class_names(class_dirs):
    names = set()
    for cf in list_class_files(class_dirs):
        for d in class_dirs:
            if cf.startswith(d.rstrip("/") + "/"):
                names.add(cf[len(d.rstrip("/")) + 1: -len(".class")])
    return names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--classes", required=True, help="comma-separated dirs of built classfiles")
    ap.add_argument(
        "--own-from",
        help="dirs used only to recognize Paimon's own classes, which it plants under "
        "`org.apache.spark.*` to reach package-private internals. Defaults to --classes; "
        "set it to the whole module output when --classes is a subset, otherwise those "
        "classes get misreported as MISSING_CLASS.",
    )
    ap.add_argument("--target-jars", required=True, help="comma-separated jars or dirs of jars")
    ap.add_argument(
        "--extra-cp",
        help="classpath for non-Spark supertypes the walk must inspect (Scala library, "
        "Hadoop, ...). JDK types resolve without it. Supertypes it cannot reach are "
        "reported as a caveat, since each one is a blind spot.",
    )
    ap.add_argument("--label", default="target Spark")
    ap.add_argument("--baseline", help="file of finding keys to subtract (from --emit-baseline)")
    ap.add_argument("--emit-baseline", help="write finding keys here instead of reporting")
    args = ap.parse_args()

    jars = []
    for p in args.target_jars.split(","):
        if os.path.isdir(p):
            for root, _, files in os.walk(p):
                jars += [os.path.join(root, f) for f in files if f.endswith(".jar")]
        else:
            jars.append(p)
    jars = sorted(set(jars))
    if not jars:
        print(f"error: no jars found under {args.target_jars}", file=sys.stderr)
        return 2

    dirs = args.classes.split(",")
    own_dirs = args.own_from.split(",") if args.own_from else dirs
    print(f"scanning {len(list_class_files(dirs))} classfiles against "
          f"{len(jars)} {args.label} jars", file=sys.stderr)
    findings, unknown = scan(dirs, jars, own_class_names(own_dirs), extra_cp=args.extra_cp)

    def fmt(k):
        return "\t".join(k)

    if args.emit_baseline:
        with open(args.emit_baseline, "w") as fh:
            fh.writelines(fmt(k) + "\n" for k in sorted(findings))
        print(f"wrote {len(findings)} baseline finding(s) to {args.emit_baseline}")
        return 0

    if args.baseline:
        with open(args.baseline) as fh:
            known = {tuple(l.rstrip("\n").split("\t")) for l in fh if l.strip()}
        findings = {k: v for k, v in findings.items() if k not in known}
        scope = f"{args.label} (new since baseline)"
    else:
        scope = args.label

    if unknown:
        shown = ", ".join(sorted(unknown)[:6])
        more = f", +{len(unknown) - 6} more" if len(unknown) > 6 else ""
        print(
            f"caveat: {len(unknown)} supertype(s) could not be inspected, so members they "
            f"declare were assumed present: {shown}{more}\n"
            f"        pass --extra-cp to close the gap.",
            file=sys.stderr,
        )

    if not findings:
        print(f"OK: no linkage problems against {scope}")
        return 0

    print(f"FOUND {len(findings)} distinct linkage problem(s) against {scope}\n")
    for k in sorted(findings):
        users = sorted(set(findings[k]))
        shown = ", ".join(users[:4]) + (f", +{len(users) - 4} more" if len(users) > 4 else "")
        print(f"  {k[0]:28s} {k[1]}.{k[2]} {k[3]}\n      used by: {shown}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
