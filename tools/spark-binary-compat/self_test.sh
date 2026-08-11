#!/usr/bin/env bash
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
# Self-test for check_linkage.py.
#
# The scanner leans on three judgement calls over `javap` output: suppress Scala mixin
# artifacts, know `java.lang.Object`'s members, and keep walking past supertypes outside
# Spark. Each is a heuristic. One that over-matches turns the scanner into a rubber stamp --
# worse than not having it, because it looks like coverage. These cases pin all three to
# classes whose linkage status was established by hand: `javap` across 4.0.3/4.1.2/4.2.0, plus
# for CatalogManager an actual IncompatibleClassChangeError from running a 4.2-built class on
# 4.0.3.
#
# Every case must fail when its filter is disabled -- verify with, e.g., replacing the
# `artifact = ...` assignment in check_linkage.py with `artifact = False`.
#
# Usage: self_test.sh <dir-of-4.0.3-jars> <dir-of-4.1.2-jars> <dir-of-4.2.0-jars>
#        (each dir holds jars or symlinks; see README for how to assemble them)
#
# Must run after `mvn -Pspark4 install -DskipTests` so target/classes exists.
set -uo pipefail

J40=${1:?usage: self_test.sh <4.0.3-jars> <4.1.2-jars> <4.2.0-jars>}
J41=${2:?}
J42=${3:?}

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
SCAN="python3 $HERE/check_linkage.py"
COMMON=$REPO/paimon-spark/paimon-spark-common/target/classes

# Spark's hierarchy inherits from Scala types; without them on the classpath the supertype
# walk gives up early and silently reports nothing (see the CatalogStorageFormat case below).
SCALA_LIB=$(ls ~/.m2/repository/org/scala-lang/scala-library/2.13.*/scala-library-2.13.*.jar 2>/dev/null | tail -1)
if [ -z "$SCALA_LIB" ]; then
  echo "no scala-library 2.13 jar in ~/.m2 (run a spark4 build first)" >&2
  exit 2
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

stage() { # stage <case-name> <class-relative-path>...
  local name=$1; shift
  local dir=$WORK/$name
  for rel in "$@"; do
    if [ ! -f "$COMMON/$rel" ]; then
      echo "MISSING BUILD OUTPUT: $rel (run: mvn -Pspark4 install -DskipTests)" >&2
      exit 2
    fi
    mkdir -p "$dir/$(dirname "$rel")"
    cp "$COMMON/$rel" "$dir/$rel"
  done
  echo "$dir"
}

fails=0
check() { # check <description> <expected-exit> <dir> <jars> [grep-pattern]
  local desc=$1 want=$2 dir=$3 jars=$4 pattern=${5:-}
  # --own-from points at the full module output: these cases stage a handful of classes, and
  # without it Paimon's own `org.apache.spark.*` classes (SparkShim, PaimonUtils, ...) would be
  # reported as MISSING_CLASS just because they are not in the staged subset.
  local out; out=$($SCAN --classes "$dir" --own-from "$COMMON" --extra-cp "$SCALA_LIB" \
      --target-jars "$jars" --label test 2>/dev/null)
  local got=$?
  if [ "$got" != "$want" ]; then
    echo "FAIL  $desc"
    echo "        expected exit $want, got $got"
    echo "$out" | sed 's/^/        /'
    fails=$((fails + 1))
    return
  fi
  if [ -n "$pattern" ] && ! grep -qE "$pattern" <<<"$out"; then
    echo "FAIL  $desc"
    echo "        exit $got as expected, but output lacks /$pattern/"
    echo "$out" | sed 's/^/        /'
    fails=$((fails + 1))
    return
  fi
  echo "ok    $desc"
}

# `SparkUtils.catalogAndIdentifier` calls three CatalogManager methods. Spark 4.2 turned
# CatalogManager from a class into an interface, so a 4.2-built call site emits
# `invokeinterface` and dies with IncompatibleClassChangeError on 4.0/4.1. Verified by running
# a synthetic 4.2-built class against 4.0.3 -- it threw. This is the scanner's reason to exist.
CM=$(stage catalogmanager org/apache/paimon/spark/SparkUtils.class)
check "CatalogManager flip reported on 4.0.3" 1 "$CM" "$J40" "KIND_FLIP interface->class.*CatalogManager"
check "CatalogManager flip reported on 4.1.2" 1 "$CM" "$J41" "KIND_FLIP interface->class.*CatalogManager"
check "same class is clean on its own baseline (4.2.0)" 0 "$CM" "$J42"

# Both of these carry references that do NOT resolve on older Spark yet cannot fail there:
#   PaimonSparkTableBase.reportDriverMetrics  -- exists only because Spark 4.2 declares the
#     member on both StagedTable and TruncatableTable, forcing Scala to emit a
#     diamond-disambiguating override. Pre-4.2 there is no diamond, so no such method.
#   PaimonBaseScanBuilder.supportsIterativePushdown -- same shape, from SupportsPushDownV2Filters.
# Neither name appears anywhere in Paimon's sources; grep confirms they are compiler-authored.
MIXIN=$(stage mixin \
  org/apache/paimon/spark/PaimonSparkTableBase.class \
  org/apache/paimon/spark/PaimonBaseScanBuilder.class)
check "Scala mixin artifacts suppressed on 4.1.2" 0 "$MIXIN" "$J41"
check "Scala mixin artifacts suppressed on 4.0.3" 0 "$MIXIN" "$J40"

# Every Spark type inherits getClass/equals/toString from java.lang.Object, which javap does
# not print as a supertype. Without the OBJECT_MEMBERS allowlist the supertype walk reports
# them on every version, drowning real findings.
# Every Spark type ultimately extends java.lang.Object, which javap does not print as a
# supertype, so the walk has to know Object's members itself. `SparkV1PartitionManagement$`
# calls `V2SessionCatalog.getClass`, and `V2SessionCatalog` declares no supertype javap can
# show -- it is the one call site in either module where that terminal is load-bearing.
OBJ=$(stage object 'org/apache/spark/sql/connector/catalog/SparkV1PartitionManagement$.class')
check "java.lang.Object members suppressed on 4.1.2" 0 "$OBJ" "$J41"
check "java.lang.Object members suppressed on 4.0.3" 0 "$OBJ" "$J40"

# The scan must NOT stop walking just because a supertype lies outside Spark.
# `CatalogStorageFormat$`'s only supertype is `java.io.Serializable`; Spark 4.2 gained a 7th
# case-class field, so a 4.2-built 6-arg construction calls `apply$default$7`, which pre-4.2
# does not have. An earlier revision treated "left the Spark jars" as "resolvable" and
# reported nothing here -- a false negative on a real, shipping incompatibility.
NONSPARK=$(stage nonspark org/apache/spark/sql/execution/PaimonDescribeTableExec.class)
check "missing member found past a non-Spark supertype (4.0.3)" 1 "$NONSPARK" "$J40" \
  'MISSING_METHOD .*CatalogStorageFormat\$\.apply\$default\$7'
check "same construction is fine on its own baseline (4.2.0)" 0 "$NONSPARK" "$J42"

# `$init$` matches the `member$` shape of a default-method forwarder but must NOT be filtered
# as one: it is the trait initializer, called unconditionally from the constructor of every
# implementing class. `PaimonDynamicPartitionOverwriteCommand` mixes in `V2WriteCommand`, which
# on 4.2 extends `WriteWithSchemaEvolution` -- a trait absent before 4.2 -- so the 4.2-built
# constructor cannot run at all on 4.0/4.1. Filtering this would hide a total failure.
INIT=$(stage init org/apache/paimon/spark/commands/PaimonDynamicPartitionOverwriteCommand.class)
check "trait initializer not treated as a mixin artifact (4.1.2)" 1 "$INIT" "$J41" \
  'MISSING_CLASS .*WriteWithSchemaEvolution'
check "same class is fine on its own baseline (4.2.0)" 0 "$INIT" "$J42"

echo
if [ "$fails" != 0 ]; then
  echo "$fails check(s) failed"
  exit 1
fi
echo "all checks passed"
