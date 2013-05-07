/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.scalding.source

import com.twitter.scalding._
import Dsl._
import cascading.tuple.Fields

abstract class DailyPrefixSuffixSource(prefixTemplate: String, suffixTemplate: String, dateRange: DateRange)
  extends TimePathedSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + suffixTemplate + "/*", dateRange, DateOps.UTC)

abstract class DailyPrefixSuffixMostRecentSource(prefixTemplate: String, suffixTemplate: String, dateRange: DateRange)
  extends MostRecentGoodSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + suffixTemplate + "/*", dateRange, DateOps.UTC)

abstract class DailySuffixSource(prefixTemplate: String, dateRange: DateRange)
  extends TimePathedSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + "/*", dateRange, DateOps.UTC)

abstract class DailySuffixMostRecentSource(prefixTemplate: String, dateRange: DateRange)
  extends MostRecentGoodSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + "/*", dateRange, DateOps.UTC)

object DailySuffixTsv {
  def apply(prefix: String, fs: Fields = Fields.ALL)
  (implicit dateRange: DateRange) = new DailySuffixTsv(prefix, fs)
}

class DailySuffixTsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends DailySuffixSource(prefix, dateRange) with DelimitedScheme {
  override val fields = fs
}

object DailySuffixCsv {
  def apply(prefix: String, fs: Fields = Fields.ALL)
  (implicit dateRange: DateRange) = new DailySuffixCsv(prefix, fs)
}

class DailySuffixCsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
extends DailySuffixSource(prefix, dateRange) with DelimitedScheme {
  override val fields = fs
  override val separator = ","
}

object DailySuffixMostRecentCsv {
  def apply(prefix: String, fs: Fields = Fields.ALL)
  (implicit dateRange: DateRange) = new DailySuffixMostRecentCsv(prefix, fs)
}

class DailySuffixMostRecentCsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends DailySuffixMostRecentSource(prefix, dateRange) with DelimitedScheme {
  override val fields = fs
  override val separator = ","
}
