# A Regular expression(Joni) parser plugin for Embulk

This is a regular expression parser plugin for Embulk.
It use [Joni](https://github.com/jruby/joni) regular expression library.
The Joni is Java port of [Oniguruma](https://github.com/kkos/oniguruma) regexp library.

The [Fluentd](https://www.fluentd.org) also use Joni/Oniguruma.
This plugin aim compatibility with [fluentd regexp parser plugin](http://docs.fluentd.org/v0.12/articles/parser-plugin-overview) format.

## Overview

* **Plugin type**: parser
* **Guess supported**: yes (trivial)

## Configuration

* **type**: Specify this parser as `joni_regexp`
* **columns**: Specify column name and type. See below (array, required)
* **stop_on_invalid_record**: Stop bulk load transaction if a file includes invalid record (such as invalid timestamp) (boolean, default: false)
* **default_timezone**: Default timezone of the timestamp (string, default: UTC)
* **default_timestamp_format**: Default timestamp format of the timestamp (string, default: `%Y-%m-%d %H:%M:%S.%N %z`)
* **newline**: Newline character (CRLF, LF or CR) (string, default: CRLF)
* **charset**: Character encoding (eg. ISO-8859-1, UTF-8) (string, default: UTF-8)
* **format**: Regular expression string [Supported expression](https://github.com/kkos/oniguruma/blob/master/doc/RE) (string, required)

### columns

* **name**: Name of the column (string, required)
* **type**: Type of the column (string, required)
* **timezone**: Timezone of the timestamp if type is timestamp (string, default: default_timestamp)
* **format**: Format of the timestamp if type is timestamp (string, default: default_format)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: joni_regexp
    columns:
      - { name: host, type: string }
      - { name: user, type: string }
      - { name: time, type: timestamp, format: "%d/%b/%Y:%H:%M:%S %z" }
      - { name: method, type: string }
      - { name: path, type: string }
      - { name: code, type: string }
      - { name: size, type: string }
      - { name: referer, type: string }
      - { name: agent, type: string }
    format: '^(?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)")?$'
```

### Guess

This plugin also support minimul `guess` command.
The `guess` command require `type` and `fomat` parameters.

`seed.yml` example.

```
in:
  type: file
  path_prefix: example/test2.txt
  parser:
    type: joni_regexp
    format: "(?<name>[^,]+),(?<birth>\\d{4}-\\d{2}-\\d{2}),(?<age>\\d+)"
out:
  type: stdout
```

execute `guess` command.

```
$ embulk guess -g joni_regexp config.yml -o guessed.yml
```

The `guess` command read `format` parameter and generate `columns`.

```
in:
  type: file
  path_prefix: example/test2.txt
  parser:
    type: joni_regexp
    format: (?<name>[^,]+),(?<birth>\d{4}-\d{2}-\d{2}),(?<age>\d+)
    charset: UTF-8
    newline: LF
    columns:
    - {name: name, type: string}
    - {name: birth, type: string}
    - {name: age, type: string}
out: {type: stdout}
```



## Install

```
$ embulk gem install embulk-parser-joni
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
