/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.physical.window;

import org.apache.samza.system.sql.Offset;

public class TimeBasedSlidingWindowAggregatorState {

  /**
   * The lower bound of the window in nano seconds (inclusive)
   */
  private final long lowerBoundNano;

  /**
   * The starting offset of the window (inclusive)
   */
  private final Offset lowerBoundOffset;

  /**
   * The upper bound of the window in nano seconds
   */
  private final long upperBoundNano;

  /**
   * Timestamp of the last seen tuple included in this window
   */
  private long lastSeenTimestampNano;

  public TimeBasedSlidingWindowAggregatorState(long lowerBoundNano, Offset lowerBoundOffset, long upperBoundNano) {
    this.lowerBoundNano = lowerBoundNano;
    this.lowerBoundOffset = lowerBoundOffset;
    this.upperBoundNano = upperBoundNano;
  }
}
