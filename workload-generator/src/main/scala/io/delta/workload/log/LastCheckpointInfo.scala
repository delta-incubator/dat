/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.workload.log

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

// `parts` is Long, not Int: the mapper's USE_LONG_FOR_INTS binds JSON integers to Long for the
// erased Option inner, and an Option[Int] field would unbox-cast that Long and throw.
/** The fields the generator reads from `_delta_log/_last_checkpoint`. */
@JsonIgnoreProperties(ignoreUnknown = true)
case class LastCheckpointInfo(version: Long, parts: Option[Long] = None)
