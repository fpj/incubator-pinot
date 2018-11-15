/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.utils.time.TimeConverter;
import com.linkedin.pinot.core.data.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


/**
 * The {@code TimeTransformer} class will convert the time value based on the {@link TimeFieldSpec}.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, time column can be treated as regular
 * column for other record transformers (incoming time column can be ignored).
 */
public class TimeTransformer implements RecordTransformer {
  private static final long MIN_VALID_TIME = new DateTime(2000, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();
  private static final long MAX_VALID_TIME = new DateTime(2050, 1, 1, 0, 0, DateTimeZone.UTC).getMillis();

  private String _incomingTimeColumn;
  private String _outgoingTimeColumn;
  private TimeConverter _incomingTimeConverter;
  private TimeConverter _outgoingTimeConverter;
  private boolean _isFirstRecord = true;

  public TimeTransformer(Schema schema) {
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

      // Perform time conversion only if incoming and outgoing granularity spec are different
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        _incomingTimeColumn = incomingGranularitySpec.getName();
        _outgoingTimeColumn = outgoingGranularitySpec.getName();
        _incomingTimeConverter = new TimeConverter(incomingGranularitySpec);
        _outgoingTimeConverter = new TimeConverter(outgoingGranularitySpec);
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    if (_incomingTimeColumn == null) {
      return record;
    }

    // Use the first record for sanity check and determine whether the conversion is needed
    Object incomingTimeValue = record.getValue(_incomingTimeColumn);
    if (_isFirstRecord) {
      _isFirstRecord = false;
      // If incoming time value does not exist or the value is invalid after conversion, check the outgoing time value.
      // If the outgoing time value is valid, skip time conversion, otherwise, throw exception.
      if (incomingTimeValue == null || !isValidTime(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue))) {
        Object outgoingTimeValue = record.getValue(_outgoingTimeColumn);
        if (outgoingTimeValue == null || !isValidTime(_outgoingTimeConverter.toMillisSinceEpoch(outgoingTimeValue))) {
          throw new IllegalStateException(
              "No valid time value found in either incoming time column: " + _incomingTimeColumn
                  + " or outgoing time column: " + _outgoingTimeColumn);
        } else {
          disableConversion();
          return record;
        }
      }
    }

    record.putField(_outgoingTimeColumn,
        _outgoingTimeConverter.fromMillisSinceEpoch(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue)));
    return record;
  }

  private void disableConversion() {
    _incomingTimeColumn = null;
    _outgoingTimeColumn = null;
    _incomingTimeConverter = null;
    _outgoingTimeConverter = null;
  }

  private static boolean isValidTime(long millisSinceEpoch) {
    return millisSinceEpoch > MIN_VALID_TIME && millisSinceEpoch < MAX_VALID_TIME;
  }
}
