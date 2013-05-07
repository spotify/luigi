# Scalding #

### Version 0.8.1 ###
* ISSUE 220: Etsy date improvements and local-mode tap improvements
* ISSUE 219: scald.rb fix
* ISSUE 218: Add aggregate method to ReduceOperations
* ISSUE 216: Improve variance notations
* ISSUE 213,215: Make Field[T] serializable
* ISSUE 210,211: Refactor date code into individual files + tests
* ISSUE 209: Add hourly/daily time pathed source classes
* ISSUE 207,208: sbt build improvements
* ISSUE 205,206: Remove scala serialization code to com.twitter.chill
* ISSUE 203: Improved date-parsing and docs (from Etsy)
* ISSUE 202,204: Add propagate/mapWithIndex in Matrix (use Monoids with graphs)
* ISSUE 201: add stdDev to groupBuilder.
* ISSUE 200: typed write and key-value swap
* ISSUE 196: Clean up deprecations
* ISSUE 194,195: Fix negative numbers as args
* ISSUE 190-192,197,199: Improved Joining/Co-grouping in the typed-API

### Version 0.8.0 ###
* Many small bugfixes
* ISSUE 189: Adds spillThreshold to GroupBuilder to tune memory usage
* ISSUE 187: Adds TypedTsv for type-safe TSV files.
* ISSUE 179: Add forceToDisk to help hand optimization of flows
* ISSUE 175: API to set the type/comparators in the Fields API.
* ISSUE 162, 163: Adds keys, values methods to Typed-API
* ISSUE 160: adds approxUniques to GroupBuilder
* ISSUE 153: mapPlusMap in GroupBuilder
* ISSUE 149: Support for Hadoop sequence files
* ISSUE 148: Matrix API
* ISSUE 147: Move Monoid/Algebra code to Algebird
* Mulitple issues: many new Kryo serializers added
* ISSUE 140: Adds ability to do side-effects (foreach, using in RichPipe)

### Version 0.7.3 ###
* ISSUE 134: Cleans up scalding.Tool
* ISSUE 133: Adds SortedListTake monoid, Either monoid, and tests
* ISSUE 130: Upgrades cascading.kryo to 0.4.4
* ISSUE 129: Disable Kryo references by default, add config
* ISSUE 128: Minor fix to normalize job

### Version 0.7.2 ###
* ISSUE 127: Upgrade maple and remove jdks.
* ISSUE 126: Switch normalize to use crossWithTiny
* ISSUE 125: Serialization fixes

### Version 0.7.1 ###
* ISSUE 124: Add crossWithSmaller and use it in normalize.
* ISSUE 123: Adds generated classes that allow tuple concatenation.

### Version 0.7.0 ###
* ISSUE 122: Upgrade cascading to version 2.0.2 and maple to 0.2.1
* ISSUE 119: Iterator fix
* ISSUE 118: Specialized tuplegetter
* ISSUE 117: Job conf keys
* ISSUE 116: Feature/flatten
* ISSUE 115: Fold scan init fix
* ISSUE 114: Feature/scald update
* ISSUE 113: Make sure SequenceFile uses the fields list if one is passed in
* ISSUE 110: Upgrade Kryo to 2.16.
* ISSUE 107: Add a method to GroupBuilder to force all aggregation on the reducers.
* ISSUE 105: Feature/head fix
* ISSUE 104: Feature/default date improvements
* ISSUE 103: Feature/case case pack
* ISSUE 100: Adds trivial UnitGroup
* ISSUE 99: Fix build breakage and add tutorial run script.

### Version 0.6.0 ###
* ISSUE 98: Feature/tpipe stream
* ISSUE 97: Add the user input to the unknown mode error message
* ISSUE 95: Feature/buffer cleanup
* ISSUE 94: Feature/tpipe
* ISSUE 93: Upgrade to cascading 2.0.0 and maple 0.2.0
* ISSUE 92: Feature/dsl refactor

### Version 0.5.4 ###
* ISSUE 91: Upgrade to cascading wip-310 and maple 0.1.10
* ISSUE 90: Test against multiple JDKs on travis-ci.org
* ISSUE 88: Bump sbt-assembly version from 0.7.3 to 0.8.1
* ISSUE 87: Header Lines support in DelimitedScheme
* ISSUE 84: Add packTo and unpackTo
* ISSUE 83: Adds sum and product to Monoid and Ring
* ISSUE 79: Updates to sbt 0.11.3 to match Travis CI

### Version 0.5.3 ###
* ISSUE 77: Add a scalding multi source tap that has a proper unique identifier

### Version 0.5.2 ###
* ISSUE 74: Upgrade cascading to wip-291 and maple to 0.1.7

### Version 0.5.1 ###
* ISSUE 73: Feature/comparator prop
* ISSUE 72: Upgrade to cascading wip-288 and maple 0.1.5
* ISSUE 71: Fixes an issue due to type erasure in KryoHadoopSerialization for scala 2.9.1
* ISSUE 70: Upgrade scalding to use cascading wip-286.
* ISSUE 69: Feature/more kryo tests

### Version 0.5.0 ###
* ISSUE 67: Upgrade cascading to wip-281.
* ISSUE 66: Allow default time zone in DefaultDateRangeJob.
* ISSUE 65: Fixed the error message thrown by FileSource.validateTaps.
* ISSUE 62: Kryo Upgrade to 2.04
* ISSUE 60: Feature/abstract algebra
* ISSUE 52: Feature/cogroup builder
* ISSUE 51: Feature/headfix

### Version 0.4.1 ###
* ISSUE 42: Feature/iterable source
* ISSUE 41: Adds blockJoinWithSmaller to JoinAlgorithms.
* ISSUE 39: Adding default value to pivot

### Version 0.4.0 ###
* ISSUE 38: Fix bug with hash code collisions of Source objects
* ISSUE 36: Some cleanups of reduce and Operations
* ISSUE 35: Split RichPipe join methods into their own trait
* ISSUE 34: Adds Pivot/Unpivot
* ISSUE 33: Add pack and unpack methods to RichPipe
* ISSUE 32: Refactors reducer setting into RichPipe
* ISSUE 31: Implemented Mode.fileExists
* ISSUE 28: Simplifies TupleConverter

### Version 0.3.5 ###
* ISSUE 21: move JobTest into main
* ISSUE 20: Adding a source for the most recent good date path.
