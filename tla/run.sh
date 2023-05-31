#!/bin/bash

set -euo pipefail

name="$1"
name="${name%.cfg}"
name="${name%.tla}"

if ! [[ -r tla2tools.jar ]]
then
        curl -OL https://github.com/tlaplus/tlaplus/releases/download/v1.7.3/tla2tools.jar
fi

java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC "${name}.tla" -config "${name}.cfg"
